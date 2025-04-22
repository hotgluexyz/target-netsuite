#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import re

import pandas as pd
from difflib import SequenceMatcher
from heapq import nlargest as _nlargest

from target_netsuite.netsuite import NetSuite
from target_netsuite.netsuite.utils import clean_logs

from netsuitesdk.internal.exceptions import NetSuiteRequestError

logger = logging.getLogger("target-netsuite")
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def get_close_matches(word, possibilities, n=20, cutoff=0.7):
    if not n >  0:
        raise ValueError("n must be > 0: %r" % (n,))
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher()
    s.set_seq2(word.lower())
    for x in possibilities:
        s.set_seq1(x.lower())
        if s.real_quick_ratio() >= cutoff and \
           s.quick_ratio() >= cutoff and \
           s.ratio() >= cutoff:
            result.append((s.ratio(), x))
    result = _nlargest(n, result)

    return {v: k for (k, v) in result}



def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    """Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", help="Config file", required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, "config_path", args.config)
        args.config = load_json(args.config)

    return args


def get_ns_client(config):
    ns_account = config.get("ns_account").upper()
    ns_consumer_key = config.get("ns_consumer_key")
    ns_consumer_secret = config.get("ns_consumer_secret")
    ns_token_key = config.get("ns_token_key")
    ns_token_secret = config.get("ns_token_secret")
    is_sandbox = config.get("is_sandbox")

    logger.info(f"Starting netsuite connection")
    ns = NetSuite(
        ns_account=ns_account,
        ns_consumer_key=ns_consumer_key,
        ns_consumer_secret=ns_consumer_secret,
        ns_token_key=ns_token_key,
        ns_token_secret=ns_token_secret,
        is_sandbox=is_sandbox,
    )

    ns.connect_tba(caching=False)
    logger.info(f"Successfully created netsuite connection..")
    return ns

def get_reference_data(ns_client, input_data):
    logger.info(f"Reading reference data from API...")
    reference_data = {}

    try:
        if "Location" in input_data.columns:
            if not input_data["Location"].dropna().empty:
                reference_data["Locations"] = ns_client.entities["Locations"](ns_client.client).get_all(["name"])
    except NetSuiteRequestError as e:
        message = e.message.replace("error", "failure").replace("Error", "")
        logger.warning(f"It was not possible to retrieve Locations data: {message}")
    
    try:
        if not input_data["Customer Name"].dropna().empty:
            reference_data["Customer"] = ns_client.entities["Customer"](ns_client.client).get_all([
                "altName",
                "name",
                "entityId",
                "companyName",
                "subsidiary",
                "isInactive"
            ])
    except NetSuiteRequestError as e:
        message = e.message.replace("error", "failure").replace("Error", "")
        logger.warning(f"It was not possible to retrieve Customer data: {message}")
    
    if not input_data["Class"].dropna().empty:
        reference_data["Classifications"] = ns_client.entities["Classifications"](ns_client.client).get_all(["name", "parent"])
    
    if not input_data["Currency"].dropna().empty:
        reference_data["Currencies"] = ns_client.entities["Currencies"](ns_client.client).get_all()

    if "Subsidiary" in input_data.columns:
        if not input_data["Subsidiary"].dropna().empty:
            reference_data["Subsidiaries"] = ns_client.entities["Subsidiaries"](ns_client.client).get_all(["name", "parent"])
    
    if "Department" in input_data.columns:
        if not input_data["Department"].dropna().empty:
            reference_data["Departments"] = ns_client.entities["Departments"](ns_client.client).get_all(["name", "parent"])

    if "SKU" in input_data.columns:
        if not input_data["SKU"].dropna().empty:
            reference_data["Items"] = ns_client.entities["Items"](ns_client.client).get_all(["itemId"])
        
    if not input_data["Account Number"].dropna().empty or not input_data["Account Name"].dropna().empty:
        reference_data["Accounts"] = ns_client.entities["Accounts"](ns_client.client).get_all(["acctName", "acctNumber", "subsidiaryList", "parent"])

    return reference_data

def log_for_journal_entry(journal_entry, ref_data):
    logger.info(f"Posting journal entry: {journal_entry}")
    try:
        subsidiaries = clean_logs(
            [s for s in ref_data['Subsidiaries'] if s['internalId'] == journal_entry['subsidiary']['internalId']],
        )
        
        logging.info(f"Subsidiary: {subsidiaries}")
    except:
        pass

    try:
        currencies = clean_logs(
            [s for s in ref_data['Currencies'] if s['internalId'] == journal_entry['currency']['internalId']],
        )
        logging.info(f"Currency: {currencies}")
    except:
        pass

    try:
        customers = clean_logs(
            [s for s in ref_data['Customer'] if s['internalId'] == journal_entry['entity']['internalId']],
        )
        logger.info(f"Customer: {customers}")
    except:
        pass

    try:
        accounts = [a['account']['internalId'] for a in journal_entry['lineList']]
        accounts = clean_logs(
            [a for a in ref_data['Accounts'] if a['internalId'] in accounts],
        )
        logger.info(f"Accounts: {accounts}")
    except:
        pass

def build_lines(x, ref_data, config):

    line_items = []
    subsidiaries = {}
    journal_subsidiary = x["Subsidiary"].iloc[0] if ref_data.get("Subsidiaries") and "Subsidiary" in x and not x.empty else None
    if journal_subsidiary:
        if isinstance(journal_subsidiary, str) and journal_subsidiary.isdigit():
            journal_subsidiary = {"internalId": journal_subsidiary}
        elif isinstance(journal_subsidiary, int) or isinstance(journal_subsidiary, float):
            journal_subsidiary = {"internalId": str(int(journal_subsidiary))}
        else:
            journal_subsidiary = [s for s in ref_data["Subsidiaries"] if s["name"] == journal_subsidiary]
            if not journal_subsidiary:
                raise Exception(f"Journal Subsidiary '{x['Subsidiary'].iloc[0]}' is not a valid subsidiary.")
            journal_subsidiary = journal_subsidiary[0]


    # Create line items
    for _, row in x.iterrows():
        #  Using Account Number if provided 
        if ref_data.get("Accounts") and row.get("Account Number") and not pd.isna(row.get("Account Number")):
            acct_num = str(row["Account Number"])
            acct_data = [a for a in ref_data["Accounts"] if a["acctNumber"] == acct_num]
            if not acct_data:
                raise ValueError(f"Account Number {row.get('Account Number')} is not found in this Netsuite account")
                 
            if len(acct_data) > 1 and row.get("Account Name"):
                logging.info(f"Multiple accounts with account number {row.get('Account Number')}, using account name to resolve")
                acct_name = str(row["Account Name"])
                acct_parent_names = [
                    s["parent"]["name"] + " : " + s["acctName"]
                    for s in ref_data["Accounts"]
                    if s.get("parent") is not None
                ]
                acct_noparent_names = [s["acctName"] for s in ref_data["Accounts"] if s.get("parent") is None]
                acct_names = acct_parent_names + acct_noparent_names
                acct_name = get_close_matches(acct_name, acct_names)
                acct_name = max(acct_name, key=acct_name.get)
                acct_data = [a for a in ref_data["Accounts"] if (a.get("parent") and (a["parent"]["name"] + " : " + a["acctName"]) == acct_name) or (a["acctName"]==acct_name)]
                if len(acct_data) == 0:
                    possible_accts = [a["acctName"] for a in ref_data["Accounts"]]
                    raise ValueError(
                        f"Account Number {row.get('Account Number')} with Account Name {row.get('Account Name')} doesn't match options. Available options are: {possible_accts}"
                    )

        # Using Account Name if provided
        elif ref_data.get("Accounts") and row.get("Account Name") and not pd.isna(row.get("Account Name")):
            acct_name = str(row["Account Name"])
            acct_data = [a for a in ref_data["Accounts"] if a["acctName"] == acct_name]
            if not acct_data:
                logger.warning(f"{acct_name} is not valid for this netsuite account, skipping line")
                continue
        else: 
            raise TypeError(f"Account Number or Account Name is required")

        acct_data = acct_data[0]
        ref_acct = {
            "name": acct_data.get("acctName"),
            "externalId": acct_data.get("externalId"),
            "internalId": acct_data.get("internalId"),
        }
        journal_entry_line = {"account": ref_acct}

        subsidiary = None
        if not pd.isna(row.get("Subsidiary")):
            row_subsidiary = row["Subsidiary"]
            # if subsidiary is a digit use it as internalId
            if isinstance(row_subsidiary, str) and row_subsidiary.isdigit():
                subsidiary = {
                    "name": None,
                    "externalId": None,
                    "internalId": row_subsidiary
                }
            elif isinstance(row_subsidiary, int) or isinstance(row_subsidiary, float):
                subsidiary = {
                    "name": None,
                    "externalId": None,
                    "internalId": str(int(row_subsidiary))
                }
            # lookup subsidiary by name
            else:
                subsidiary_parent_names = [
                    s["parent"]["name"] + " : " + s["name"]
                    for s in ref_data["Subsidiaries"]
                    if s.get("parent") is not None
                ]
                subsidiary_noparent_names = [s["name"] for s in ref_data["Subsidiaries"] if s.get("parent") is None]
                subsidiary_names = subsidiary_parent_names + subsidiary_noparent_names
                subsidiary_name = get_close_matches(row["Subsidiary"], subsidiary_names)

                ## secondary check for Subsidiary names alone if no match
                subsidiary_names = [s["name"] for s in ref_data['Subsidiaries']]
                subsidiary_name.update(get_close_matches(row['Subsidiary'],subsidiary_names))

                if subsidiary_name:
                    subsidiary_name = max(subsidiary_name, key=subsidiary_name.get)
                    subsidiary_data = [s for s in ref_data["Subsidiaries"] if (s.get("parent") and (s["parent"]["name"] + " : " + s["name"]) == subsidiary_name) or (s["name"]==subsidiary_name)]
                # pass subsidiary data to subsidiary
                    if subsidiary_data:
                        subsidiary_data = subsidiary_data[0]
                        subsidiary = {
                            "name": None,
                            "externalId": None,
                            "internalId": subsidiary_data.get("internalId"),
                        }
        # Extract the subsidiaries from Account
        else:
            if acct_data['subsidiaryList']:
                if isinstance(acct_data['subsidiaryList'], list):
                    subsidiary = acct_data['subsidiaryList'][0]
                else:
                    subsidiary = acct_data['subsidiaryList']['recordRef']
                    subsidiary = subsidiary[0] if subsidiary else None
            else:
                subsidiary = None
        if subsidiary:
            if row["Posting Type"].lower() == "credit":
                subsidiaries["toSubsidiary"] = subsidiary
            elif row["Posting Type"].lower() == "debit":
                subsidiaries["subsidiary"] = subsidiary
            else:
                raise('Posting Type must be "credit" or "debit"')

        # Get the NetSuite Class Ref
        if ref_data.get("Classifications") and row.get("Class") and not pd.isna(row.get("Class")):
            class_parent_names = [
                c["parent"]["name"] + " : " + c["name"]
                for c in ref_data["Classifications"]
                if c.get("parent") is not None
            ]
            class_noparent_names = [c["name"] for c in ref_data["Classifications"] if c.get("parent") is None]
            class_names = class_parent_names + class_noparent_names
            class_name = get_close_matches(row["Class"], class_names)
            if class_name:
                class_name = max(class_name, key=class_name.get)
                class_data = [c for c in ref_data["Classifications"] if (c.get("parent") and (c["parent"]["name"] + " : " + c["name"]) == class_name) or (c["name"]==class_name)]
                if class_data:
                    class_data = class_data[0]
                    journal_entry_line["class"] = {
                        "name": class_data.get("name"),
                        "externalId": class_data.get("externalId"),
                        "internalId": class_data.get("internalId"),
                    }

        # Get the NetSuite Department Ref
        if ref_data.get("Departments") and row.get("Department") and not pd.isna(row.get("Department")):
            department_parent_names = [
                c["parent"]["name"] + " : " + c["name"]
                for c in ref_data["Departments"]
                if c.get("parent") is not None
            ]
            department_noparent_names = [d["name"] for d in ref_data["Departments"] if d.get("parent") is None]
            dept_names = department_parent_names + department_noparent_names
            dept_name = get_close_matches(row["Department"], dept_names)
            if dept_name:
                dept_name = max(dept_name, key=dept_name.get)
                dept_data = [d for d in ref_data["Departments"] if (d.get("parent") and (d["parent"]["name"] + " : " + d["name"]) == dept_name) or (d["name"] == dept_name)]
                if dept_data:
                    dept_data = dept_data[0]
                    journal_entry_line["department"] = {
                        "name": dept_data.get("name"),
                        "externalId": dept_data.get("externalId"),
                        "internalId": dept_data.get("internalId"),
                    }

        # Get the NetSuite Location Ref
        if ref_data.get("Locations") and row.get("Location") and not pd.isna(row.get("Location")):
            loc_data = [l for l in ref_data["Locations"] if l["name"] == row["Location"]]
            if loc_data:
                loc_data = loc_data[0]
                journal_entry_line["location"] = {
                    "name": loc_data.get("name"),
                    "externalId": loc_data.get("externalId"),
                    "internalId": loc_data.get("internalId"),
                }

        # Get the NetSuite Location Ref
        customer_name = row.get("Customer Name")
        customer_id = row.get("Customer Id") if row.get("Customer Id") else row.get("Customer ID")
        
        if ref_data.get("Customer") and not (pd.isna(customer_name) and pd.isna(customer_id)):
            if customer_id: 
                # Search for the customer based on the customer id
                # and removes inactive customers so the line is skipped
                customer = list(
                    filter(
                        lambda x: (x['internalId'] == str(customer_id) or x['entityId'] == str(customer_id)) and (x["isInactive"] == False),
                        ref_data['Customer']
                    )
                )
                customer_log = clean_logs(customer)
                logger.info(f"Customers found for id '{customer_id}': {customer_log}")
                if journal_subsidiary:
                    customer = [c for c in customer if c["subsidiary"]["internalId"] == journal_subsidiary["internalId"]]

                if len(customer) > 1 and customer_name:
                    # If customer id is duplicated, search for the customer based on the customer name
                    customer = list(filter(lambda x: x.get('companyName') == customer_name, customer))
            
            if not customer_id or not customer:
                customer_names = []
                for c in ref_data["Customer"]:
                    if c.get("isInactive"):
                        continue
                    if c.get("name"):
                        customer_names.append(c["name"])
                    if c.get("entityId"):
                        customer_names.append(c["entityId"])
                    if c.get("altName"):
                        customer_names.append(c["altName"])
                    if c.get("companyName"):
                        customer_names.append(c["companyName"])
                # only get close matches if the exact same name is not present in customers
                if customer_name in customer_names:
                    customer_name = {customer_name: 1}
                else:
                    customer_name = get_close_matches(row["Customer Name"], customer_names, n=2, cutoff=0.95)
                if customer_name:
                    customer_name = max(customer_name, key=customer_name.get)
                    customer_data = []
                    for c in ref_data["Customer"]:
                        if "name" in c.keys():
                            if c["name"] == customer_name:
                                customer_data.append(c)
                        if "entityId" in c.keys():
                            if c["entityId"] == customer_name:
                                customer_data.append(c)
                        if "altName" in c.keys():
                            if c["altName"] == customer_name:
                                customer_data.append(c)
                        if "companyName" in c.keys():
                            if c["companyName"] == customer_name:
                                customer_data.append(c)
                    if not customer_data:
                        raise Exception(f"Customer {customer_name} was not found")

                    customer_data_log = clean_logs(customer_data)
                    logger.info(f"Customers found for customer name '{customer_name}': {customer_data_log}")
                    if journal_subsidiary:
                        customer_data = [c for c in customer_data if c["subsidiary"]["internalId"] == journal_subsidiary["internalId"]]
                    if customer_data:
                        customer_data = customer_data[0]
                        journal_entry_line["entity"] = {
                            "externalId": customer_data.get("externalId"),
                            "internalId": customer_data.get("internalId"),
                        }
                    else:
                        raise Exception(f"Customer with name {customer_name} or id {customer_id} doesn't belong to journal subsidiary {journal_subsidiary['internalId']} with id {journal_subsidiary['name']}")
            else: 
                journal_entry_line['entity'] = { 
                    "externalId": customer[0].get("externalId"),
                    "internalId": customer[0].get("internalId")
                }

        custom_field_values = []

        if row.get("SKU") and not pd.isna(row.get("SKU")) and config.get("sku_custom_field"):
            external_id = config.get("sku_custom_field")
            if config.get("sku_item_lookup", False):
              item_id = next((i["internalId"] for i in ref_data["Items"] if (i["externalId"]==row["SKU"] or i['itemId'] == row["SKU"])), None)
              if item_id:
                   custom_field_values.append([{"type": "Select", "scriptId": external_id, "value": item_id}])
            else:
              custom_field_values.append([{"type": "Select", "scriptId": external_id, "value": row['SKU']}])

        # Support dynamic custom fields
        custom_fields = config.get("custom_fields") or []

        for entry in custom_fields:
            value = row.get(entry.get("input_id"))
            ns_id = entry.get("netsuite_id")
            if value:
                custom_field_values.append([{"type": "Select", "scriptId": ns_id, "value": value}])

        if custom_field_values:
            journal_entry_line["customFieldList"] = custom_field_values

        # Check the Posting Type and insert the Amount
        amount = 0 if pd.isna(row["Amount"]) else abs(round(row["Amount"], 2))
        if row["Posting Type"].lower() == "credit":
            journal_entry_line["credit"] = amount
        elif row["Posting Type"].lower() == "debit":
            journal_entry_line["debit"] = amount

        # Insert the Journal Entry to the memo field
        if "Description" in x.columns:
            journal_entry_line["memo"] = row["Description"]
        
        line_items.append(journal_entry_line)

    # Get the currency ID
    if ref_data.get("Currencies") and row.get("Currency"):
        currency_data = [
            c for c in ref_data["Currencies"] if c["symbol"] == row["Currency"]
            ]
        if not currency_data:
            raise Exception(f"Currency '{row['Currency']}' not found")
        if currency_data:
            currency_data = currency_data[0]
            currency_ref = {
                "name": currency_data.get("symbol"),
                "externalId": currency_data.get("externalId"),
                "internalId": currency_data.get("internalId"),
            }
    else:
        currency_ref = None

    # Check if subsidiary is duplicated and delete toSubsidiary if true
    if len(subsidiaries)>1:
        if subsidiaries['subsidiary'] == subsidiaries['toSubsidiary']:
            del subsidiaries['toSubsidiary']

    if "Transaction Date" in x.columns:
        created_date = pd.to_datetime(x["Transaction Date"].iloc[0])
    else:
        created_date = None

    # Create the journal entry
    journal_entry = {
        "createdDate": created_date,
        "tranDate": created_date,
        "externalId": x["Journal Entry Id"].iloc[0],
        "lineList": line_items,
        "currency": currency_ref
    }

    if "JournalDesc" in x.columns:
        journal_entry["memo"] = "" if pd.isnull(x["JournalDesc"].iloc[0]) else x["JournalDesc"].iloc[0]
    
    # Update the entry with subsidiaries
    journal_entry.update(subsidiaries)
    log_for_journal_entry(journal_entry, ref_data)
    return journal_entry


def load_journal_entries(input_data, reference_data, config):
    # Build the entries
    try:
        if "Journal Entry Id" in input_data.columns and "Subsidiary" in input_data.columns:
            lines = input_data.groupby(["Journal Entry Id",'Subsidiary']).apply(build_lines, reference_data, config)
        else:
            # Assuming Journal Entry Id will always be present
            lines = input_data.groupby(["Journal Entry Id"]).apply(build_lines, reference_data, config)
    except RuntimeError as e:
        raise Exception("Building Netsuite JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(lines)} journal entries to post")

    return lines.values


def post_journal_entries(journal, ns_client, ref_data):
        entity = "JournalEntry"
        # logger.info(f"Posting data for entity {1}")
        try:
            response = ns_client.entities[entity](ns_client.client).post(journal)
            return json.dumps({entity: response}, default=str, indent=2)
        except Exception as e:
            match = re.search(r"Invalid entity reference key (\d+) for subsidiary (\d+)", e.__str__())
            if match:
                entity_id = match.group(1)
                subsidiary_id = match.group(2)
                entity = [c for c in ref_data["Customer"] if c["internalId"] == entity_id]
                entity_name = entity[0]["companyName"] if entity else ""
                subsidiary = [s for s in ref_data["Subsidiaries"] if s["internalId"] == subsidiary_id]
                subsidiary_name = subsidiary[0]["name"] if entity else ""
                error_message = f"Customer '{entity_name}' with id '{entity_id}' cannot be used with subsidiary '{subsidiary_name}' with id '{subsidiary_id}'"
                raise Exception(error_message)
            raise e


def read_input_data(config):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    input_data = pd.read_csv(input_path, keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'NULL', 'NaN', 'n/a', 'nan', 'null',"\\N"])
    cols = list(input_data.columns)
    REQUIRED_COLS = [
        "Transaction Date",
        "Journal Entry Id",
        "Customer Name",
        "Class",
        "Account Number",
        "Account Name",
        "Posting Type",
        "Description",
    ]
    # Verify it has required columns
    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is mising REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )
        sys.exit(1)
    # replace nan with None
    input_data = input_data.where(pd.notnull(input_data), None)
    return input_data

def upload_journals(config, ns_client):
    # Read input data
    input_data = read_input_data(config)
    
    # Load reference data
    reference_data = get_reference_data(ns_client, input_data)
    # Load Journal Entries CSV to post + Convert to NetSuite format
    journals = load_journal_entries(input_data, reference_data, config)

    # Post the journal entries to Netsuite
    for journal in journals:
        logger.info(f"Posting journal: {journal}")
        response = post_journal_entries(journal, ns_client, reference_data)
        logger.info(f"Posted journal: {json.dumps(response, default=str)}")

    logger.info(f"Posted journal entries: ")
    logger.info(f"{json.dumps(journals,default=str)}")

def upload(config, args):
    # Login to NetSuite
    ns = get_ns_client(config)
    ns_client = ns.ns_client

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
