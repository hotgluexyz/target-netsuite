#!/usr/bin/env python3
import os
import json
import sys
import argparse
import requests
import base64
import pandas as pd
import logging
import re

logger = logging.getLogger("target-netsuite")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def write_json_file(filename, content):
    with open(filename, 'w') as f:
        json.dump(content, f, indent=4)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def get_ns_client(config):
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


def get_reference_data(ns_client):
    customers = ns_client.entities['Customer'].get_all()
    accounts = ns_client.entities['Accounts'].get_all()
    classifications = ns_client.entities['Classifications'].get_all()

    return {
        'Customers': json.loads(json.dumps(customers, default=str, indent=2)),
        'Accounts': json.loads(json.dumps(accounts, default=str, indent=2)),
        'Classifications': json.loads(json.dumps(classifications, default=str, indent=2))
    }


def load_journal_entries(config, reference_data):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    df = pd.read_csv(input_path)
    # Verify it has required columns
    cols = list(df.columns)
    REQUIRED_COLS = ["Transaction Date", "Journal Entry Id", "Customer Name", "Class", "Account Number", "Account Name", "Posting Type", "Description"]

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(f"CSV is mising REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}")
        sys.exit(1)

    journal_entries = []
    errored = False

    def build_lines(x):
        # Get the journal entry id
        je_id = x['Journal Entry Id'].iloc[0]
        logger.info(f"Converting {je_id}...")
        line_items = []

        # Create line items
        for index, row in x.iterrows():
            # Create journal entry line detail
            je_detail = {
                "PostingType": row['Posting Type']
            }

            # Get the Quickbooks Account Ref
            acct_num = str(row['Account Number'])
            acct_name = row['Account Name']
            acct_ref = accounts.get(acct_num, accounts.get(acct_name, {})).get("Id")

            if acct_ref is not None:
                je_detail["AccountRef"] = {
                    "value": acct_ref
                }
            else:
                errored = True
                logger.error(f"Account is missing on Journal Entry {je_id}! Name={acct_name} No={acct_num}")

            # Get the Quickbooks Class Ref
            class_name = row['Class']
            class_ref = classes.get(class_name, {}).get("Id")

            if class_ref is not None:
                je_detail["ClassRef"] = {
                    "value": class_ref
                }
            else:
                logger.warning(f"Class is missing on Journal Entry {je_id}! Name={class_name}")

            # Get the Quickbooks Customer Ref
            customer_name = row['Customer Name']
            customer_ref = customers.get(customer_name, {}).get("Id")

            if customer_ref is not None:
                je_detail["Entity"] = {
                    "EntityRef": {
                        "value": customer_ref
                    },
                    "Type": "Customer"
                }
            else:
                logger.warning(f"Customer is missing on Journal Entry {je_id}! Name={customer_name}")

            # Create the line item
            line_items.append({
                "Description": row['Description'],
                "Amount": row['Amount'],
                "DetailType": "JournalEntryLineDetail",
                "JournalEntryLineDetail": je_detail
            })

        # Create the entry
        entry = {
            'TxnDate': row['Transaction Date'],
            'DocNumber': je_id,
            'Line': line_items
        }

        # Append the currency if provided
        if row.get('Currency') is not None:
            entry['CurrencyRef'] = {
                'value': row['Currency']
            }

        journal_entries.append(entry)

    # Build the entries
    df.groupby("Journal Entry Id").apply(build_lines)

    if errored:
        raise Exception("Building QBO JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(journal_entries)} journal entries to post")

    return journal_entries


# def post_journal_entries(journals, ns_client):


def upload_journals(config, ns_client):
    # Load reference data
    reference_data = get_reference_data(ns_client)

    # Load Journal Entries CSV to post + Convert to NetSuite format
    journals = load_journal_entries(config, reference_data)

    # Post the journal entries to Quickbooks
    # post_journal_entries(journals, security_context)


def upload(config, args):
    # Login to NetSuite
    ns = get_ns_client(config)
    ns_client = ns.client

    if os.path.exists(f"{config['input_path']}/JournalEntries.csv"):
        logger.info("Found JournalEntries.csv, uploading...")
        upload_journals(config, ns_client)
        logger.info("JournalEntries.csv uploaded!")

    logger.info("Posting process has completed!")


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the new QBO data
    upload(args.config, args)


if __name__ == "__main__":
    main()
