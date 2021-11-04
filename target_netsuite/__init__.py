#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

from target_netsuite.netsuite import NetSuite

logger = logging.getLogger("target-netsuite")
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    """Parse standard command-line args.
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

    # enable this when using local
    # s.connect_tba(caching=True)

    ns.connect_tba(caching=False)
    logger.info(f"Successfully created netsuite connection..")
    return ns


def get_reference_data(ns_client):
    accounts = ns_client.entities["Accounts"].get_all()
    classifications = ns_client.entities["Classifications"].get_all()
    currencies = ns_client.currencies.get_all()
    departments = ns_client.departments.get_all()
    locations = ns_client.locations.get_all()

    return {
        "Accounts": json.loads(json.dumps(accounts, default=str, indent=2)),
        "Currencies": json.loads(json.dumps(currencies, default=str, indent=2)),
        "Classifications": json.loads(json.dumps(classifications, default=str, indent=2)),
        "Departments": json.loads(json.dumps(departments, default=str, indent=2)),
        "Locations": json.loads(json.dumps(locations, default=str, indent=2)),
    }


def build_lines(x, ref_data):

    line_items = []
    subsidiaries = {}
    # Create line items
    for _, row in x.iterrows():
        # Get the NetSuite Account Ref
        acct_num = str(row["Account Number"])
        acct_data = next(a for a in ref_data["Accounts"] if a["acctNumber"] == acct_num)
        ref_acct = {
            "name": acct_data.get("name"),
            "externalId": acct_data.get("externalId"),
            "internalId": acct_data.get("internalId"),
        }
        journal_entry_line = {"account": ref_acct}
        
        # Extract the subsidiaries from Account
        subsidiary = acct_data['subsidiaryList']['recordRef']
        if subsidiary:
            subsidiary = subsidiary[0]
            if row["Posting Type"] == "Credit":
                subsidiaries["toSubsidiary"] = subsidiary
            elif row["Posting Type"] == "Debit":
                subsidiaries["subsidiary"] = subsidiary

        # Get the NetSuite Class Ref
        
        class_data = [d for d in ref_data["Classifications"] if row["Class"] in d["name"].split(" - ")]
        if class_data:
            journal_entry_line["class"] = {
                "name": class_data[0].get("name"),
                "externalId": class_data[0].get("externalId"),
                "internalId": class_data[0].get("internalId"),
            }

        # Get the NetSuite Department Ref
        dept_data = [d for d in ref_data["Departments"] if row["Department"] in d["name"].split(" - ")]
        if dept_data:
            journal_entry_line["department"] = {
                "name": dept_data[0].get("name"),
                "externalId": dept_data[0].get("externalId"),
                "internalId": dept_data[0].get("internalId"),
            }

        # Get the NetSuite Location Ref
        loc_data = [l for l in ref_data["Locations"] if l["name"] == row["Location"]]
        if loc_data:
            journal_entry_line["location"] = {
                "name": loc_data[0].get("name"),
                "externalId": loc_data[0].get("externalId"),
                "internalId": loc_data[0].get("internalId"),
            }

        # Check the Posting Type and insert the Amount
        if row["Posting Type"] == "Credit":
            journal_entry_line["credit"] = round(row["Amount"], 2)
        elif row["Posting Type"] == "Debit":
            journal_entry_line["debit"] = round(row["Amount"], 2)

        # Insert the Journal Entry to the memo field
        journal_entry_line["memo"] = x["Description"].iloc[0]
        
        line_items.append(journal_entry_line)

    # Get the currency ID
    currency_data = [
        c for c in ref_data["Currencies"] if c["symbol"] == row["Currency"]
        ]
    if currency_data:
        currency_ref = {
            "name": currency_data[0].get("symbol"),
            "externalId": currency_data[0].get("externalId"),
            "internalId": currency_data[0].get("internalId"),
        }
    else:
        currency_ref = None

    # Check if subsidiary is duplicated and delete toSubsidiary if true
    if len(subsidiaries)>1:
        if subsidiaries['subsidiary'] == subsidiaries['toSubsidiary']:
            del subsidiaries['toSubsidiary']

    date_str = x["Transaction Date"].iloc[0]
    # Create the journal entry
    journal_entry = {
        "createdDate": datetime.strptime(date_str, "%m/%d/%y"),
        "tranDate": datetime.strptime(date_str, "%m/%d/%y"),
        "externalId": x["Journal Entry Id"].iloc[0],
        "lineList": line_items,
        "currency": currency_ref
    }
    
    # Update the entry with subsidiaries
    journal_entry.update(subsidiaries)

    return journal_entry


def load_journal_entries(config, reference_data):
    # Get input path
    input_path = f"{config['input_path']}/JournalEntries.csv"
    # Read the passed CSV
    df = pd.read_csv(input_path)
    # Verify it has required columns
    cols = list(df.columns)
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

    if not all(col in cols for col in REQUIRED_COLS):
        logger.error(
            f"CSV is mising REQUIRED_COLS. Found={json.dumps(cols)}, Required={json.dumps(REQUIRED_COLS)}"
        )
        sys.exit(1)

    # Build the entries
    try:
        lines = df.groupby(["Journal Entry Id"]).apply(build_lines, reference_data)
    except RuntimeError as e:
        raise Exception("Building QBO JournalEntries failed!")

    # Print journal entries
    logger.info(f"Loaded {len(lines)} journal entries to post")

    return lines.values


def post_journal_entries(journals, ns_client):
    for data_to_post in journals:
        entity = "JournalEntry"
        # logger.info(f"Posting data for entity {1}")
        response = ns_client.entities[entity].post(data_to_post)
        return json.dumps({entity: response}, default=str, indent=2)


def upload_journals(config, ns_client):
    # Load reference data
    reference_data = get_reference_data(ns_client)

    # Load Journal Entries CSV to post + Convert to NetSuite format
    journals = load_journal_entries(config, reference_data)

    # Post the journal entries to Quickbooks
    post_journal_entries(journals, ns_client)


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
