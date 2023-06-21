#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys

import pandas as pd
from difflib import SequenceMatcher
from heapq import nlargest as _nlargest

from target_netsuite.netsuite import NetSuite

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
    s.set_seq2(word)
    for x in possibilities:
        s.set_seq1(x)
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
    ns_account = config.get("ns_account")
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
            reference_data["Customer"] = ns_client.entities["Customer"](ns_client.client).get_all(["altName", "name", "entityId", "companyName"])
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
        reference_data["Accounts"] = ns_client.entities["Accounts"](ns_client.client).get_all(["acctName", "acctNumber", "subsidiaryList"])

    return reference_data


def build_lines(x, ref_data, config):

    line_items = []
    subsidiaries = {}
    # Create line items
    for _, row in x.iterrows():
        #  Using Account Number if provided 
        if ref_data.get("Accounts") and row.get("Account Number") and not pd.isna(row.get("Account Number")):
            acct_num = str(row["Account Number"])
            acct_data = [a for a in ref_data["Accounts"] if a["acctNumber"] == acct_num]
            if not acct_data:
                logger.warning(f"{acct_num} is not valid for this netsuite account, skipping line")
                continue

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

        # Get subsidiary
        if not pd.isna(row.get("Subsidiary")):
            subsidiary_parent_names = [
                s["parent"]["name"] + " : " + s["name"]
                for s in ref_data["Subsidiaries"]
                if s.get("parent") is not None
            ]
            subsidiary_noparent_names = [s["name"] for s in ref_data["Subsidiaries"] if s.get("parent") is None]
            subsidiary_names = subsidiary_parent_names + subsidiary_noparent_names
            subsidiary_name = get_close_matches(row["Subsidiary"], subsidiary_names)
            if not subsidiary_name: ## secondary check for Subsidiary names alone if no match
                subsidiary_names = [s["name"] for s in ref_data['Subsidiaries']]
                subsidiary_name = get_close_matches(row['Subsidiary'],subsidiary_names)
            
            if subsidiary_name:
                subsidiary_name = max(subsidiary_name, key=subsidiary_name.get)
                subsidiary_data = [s for s in ref_data["Subsidiaries"] if (s.get("parent") and (s["parent"]["name"] + " : " + s["name"]) == subsidiary_name) or (s["name"]==subsidiary_name)]
                if subsidiary_data:
                    subsidiary_data = subsidiary_data[0]
                    subsidiary = {
                        "name": None,
                        "externalId": None,
                        "internalId": subsidiary_data.get("internalId"),
                    }
                else:
                    subsidiary = None
            else:
                subsidiary = None
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
        customer_id = row.get("Customer ID")
        if ref_data.get("Customer") and not (pd.isna(customer_name) and pd.isna(customer_id)):
            if customer_id: 
                customer = list(filter(lambda x: x['entityId'] == customer_id, ref_data['Customer']))
            
            if not customer_id or not customer:
                customer_names = []
                for c in ref_data["Customer"]:
                    if c.get("name"):
                        customer_names.append(c["name"])
                    if c.get("entityId"):
                        customer_names.append(c["entityId"])
                    if c.get("altName"):
                        customer_names.append(c["altName"])
                    if c.get("companyName"):
                        customer_names.append(c["companyName"])
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
                    if customer_data:
                        customer_data = customer_data[0]
                        journal_entry_line["entity"] = {
                            "externalId": customer_data.get("externalId"),
                            "internalId": customer_data.get("internalId"),
                        }
            else: 
                journal_entry_line = { 
                    "externalId": customer[0].get("externalId"),
                    "internalId": customer[0].get("internalId")
                }

        if row.get("SKU") and not pd.isna(row.get("SKU")) and config.get("sku_custom_field"):
            external_id = config.get("sku_custom_field")
            if config.get("sku_item_lookup", False):
              item_id = next((i["internalId"] for i in ref_data["Items"] if (i["externalId"]==row["SKU"] or i['itemId'] == row["SKU"])), None)
              if item_id:
                  journal_entry_line["customFieldList"] = [{"type": "Select", "scriptId": external_id, "value": item_id}]
            else:
              journal_entry_line["customFieldList"] = [{"type": "Select", "scriptId": external_id, "value": row['SKU']}]

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

    return journal_entry


def load_journal_entries(input_data, reference_data, config):
    # Build the entries
    try:
        lines = input_data.groupby(["Journal Entry Id"]).apply(build_lines, reference_data, config)
    except RuntimeError as e:
        raise Exception("Building Netsuite JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(lines)} journal entries to post")

    return lines.values


def post_journal_entries(journal, ns_client):
        entity = "JournalEntry"
        # logger.info(f"Posting data for entity {1}")
        response = ns_client.entities[entity](ns_client.client).post(journal)
        return json.dumps({entity: response}, default=str, indent=2)


def read_input_data(config):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    input_data = pd.read_csv(input_path, keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
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
        post_journal_entries(journal, ns_client)

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
