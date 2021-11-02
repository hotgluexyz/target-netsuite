#!/usr/bin/env python3

import boto3
import json
import singer
from concurrent import futures
from botocore.config import Config
from botocore.exceptions import ClientError
from tap_netsuite.netsuite import NetSuite

logger = singer.get_logger()


def __get_ns_client__(config):
    ns_account = config.get('ns_account')
    ns_consumer_key = config.get('ns_consumer_key')
    ns_consumer_secret = config.get('ns_consumer_secret')
    ns_token_key = config.get('ns_token_key')
    ns_token_secret = config.get('ns_token_secret')
    is_sandbox = config.get('is_sandbox')

    logger.info(f"Starting netsuite connection")
    ns = NetSuite(ns_account=ns_account,
                  ns_consumer_key=ns_consumer_key,
                  ns_consumer_secret=ns_consumer_secret,
                  ns_token_key=ns_token_key,
                  ns_token_secret=ns_token_secret,
                  is_sandbox=is_sandbox)

    # enable this when using local
    # ns.connect_tba(caching=True)

    ns.connect_tba(caching=False)
    logger.info(f"Successfully created netsuite connection..")
    return ns


def get_object_json(bucket, s3_keys, key, region):
    try:
        s3_client = boto3.client('s3', config=Config(
            region_name=region
        ))
        key = s3_keys.get(key)
        body = s3_client.get_object(Bucket=bucket, Key=key).get("Body")
        if body is not None:
            return json.loads(body.read().decode('utf-8'))
        else:
            return None

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            logger.error("Unexpected error: %s" % e)
            raise

    except Exception as e:
        logger.error(e)
        return None


def read_all_files(s3_config):
    s3_bucket = s3_config.get('s3_bucket')
    s3_region = s3_config.get('s3_region', 'us-east-1')
    s3_keys = s3_config.get('keys')

    logger.info(f"s3_bucket is {s3_bucket}")

    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_key = {executor.submit(get_object_json, s3_bucket, s3_keys, key, s3_region): key for key in
                         s3_keys.keys()}

        logger.info(f"All files {s3_keys} submitted for read")

        for future in futures.as_completed(future_to_key):

            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield key, future.result()
            else:
                logger.error(f"Error occurred while reading file {key} {s3_keys[key]}")
                raise exception


def find(pred1, pred2, iterable):
    for element in iterable:
        if pred1(element):
            return element
        elif pred2 is not None and pred2(element):
            return element
    return None


def __populate_reference_data__(data, reference_data):
    line_list = []
    for lineIndex in range(len(data.get('lines', []))):
        line = data.get('lines')[lineIndex]
        source_account = line.get('account')
        source_customer = line.get('customer')
        source_class = line.get('class')

        account = find(lambda x: x.get("acctNumber") == source_account, None, reference_data.get('Accounts', []))
        if account is not None:
            data.get('lines')[lineIndex]['account'] = {
                "name": f"{account.get('acctName')}",
                "internalId": f"{account.get('internalId')}"
            }
            customer = find(lambda x: x.get("entityId") == source_customer,
                            lambda x: x.get("companyName") == source_customer,
                            reference_data.get('Customers', []))
            data.get('lines')[lineIndex].pop('customer')
            if customer is not None:
                data['entity'] = {
                    "name": f"{customer.get('entityId')}",
                    "internalId": f"{customer.get('internalId')}"
                }

            if source_class is not None:
                classification = find(lambda x: x.get("name") == source_class,
                                      None,
                                      reference_data.get('Classifications', []))
                if classification is not None:
                    data.get('lines')[lineIndex]['class'] = {
                        'name': classification.get('name'),
                        'internalId': classification.get('internalId')
                    }
                else:
                    data.get('lines')[lineIndex]['class'] = None

            credit = data.get('lines')[lineIndex]['credit']

            debit = data.get('lines')[lineIndex]['debit']

            if debit is not None:
                debit = abs(debit)
                data.get('lines')[lineIndex]['debit'] = round(debit, 2)

            if credit is not None:
                credit = abs(credit)
                data.get('lines')[lineIndex]['credit'] = round(credit, 2)

            line_list.append(data.get('lines')[lineIndex])
        else:
            logger.error(
                f"Account {source_account} for line {line} does not exists in NetSuite. Aborting reference data population.")
            # Raise error because we can't post this journal entry as it does not have
            # account number in netsuite

    data['lines'] = line_list


def post(ns, entity, data, reference_data):
    logger.info(f"Posting data for entity {entity}")
    data_to_post = data[entity]
    __populate_reference_data__(data_to_post, reference_data)
    # data_to_post['lineList'] = []
    # data_to_post['lineList'].extend(data_to_post.get('lines'))
    # data_to_post['lineList'].extend(data_to_post.get('lines'))
    # data_to_post.pop('lines')
    response = ns.ns_client.entities[entity].post(data_to_post)
    return json.dumps({entity: response}, default=str, indent=2)


def post_data(ns, data_to_post, reference_data):
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_key = {executor.submit(post, ns, key, data_to_post, reference_data): key for key in
                         data_to_post.keys()}

        for future in futures.as_completed(future_to_key):

            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                logger.info(f"Successfully posted data for entity {key}")
                yield key, future.result()
            else:
                logger.error(f"Failed to post data for entity {key}. exception {exception}")
                raise exception


def __get_reference_data(ns_client):
    customers = ns_client.entities['Customer'].get_all()
    accounts = ns_client.entities['Accounts'].get_all()
    classifications = ns_client.entities['Classifications'].get_all()

    return {
        'Customers': json.loads(json.dumps(customers, default=str, indent=2)),
        'Accounts': json.loads(json.dumps(accounts, default=str, indent=2)),
        'Classifications': json.loads(json.dumps(classifications, default=str, indent=2))
    }


def execute_lambda(event):
    s3_config = event.get('s3_config')
    ns = None
    response = []
    try:
        data_to_write = {}
        for key, result in read_all_files(s3_config):
            logger.info(f"Successfully read data for entity {key}")
            data_to_write[key] = result

        netsuite_config = event.get('netsuite_config')
        ns = __get_ns_client__(netsuite_config)

        reference_data = __get_reference_data(ns.ns_client)

        for entity, _response in post_data(ns, data_to_write, reference_data):
            response.append(_response)
    except Exception as e:
        raise e
    finally:
        if ns is not None:
            ns.ns_client.client.logout()

    return response
