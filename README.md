# target-netsuite

[NetSuite](https://www.netsuite.com/) target that inserts JournalEntries from a CSV file into the Netsuite API.

```bash
$ python3 -m venv env/target-netsuite
$ source env/target-netsuite/bin/activate
$ pip install .
$ target-netsuite --config config.json
```

# Quickstart

## Install the target

```
> pip install target-netsuite
```

## Create a Config file
#### Token Based Authentication
```
{
    "input_path": "input_path",
    "is_sandbox": true / false,
    "ns_account": "netsuite_account_id",
    "ns_consumer_key": "netsuite_consumer_key",
    "ns_consumer_secret": "netsuite_consumer_secret",
    "ns_token_key": "netsuite_token_key",
    "ns_token_secret": "netsuite_token_secret",
}

```
The `input_path` is the path for the input JournalEntries csv file.

The `is_sandbox` should always be set to "true" if you are connecting Production account of NetSuite. Set it to false if you want to connect to SandBox acccount. 

The `ns_account` is your account Id. This can be found under Setup -> Company -> Company Information. Look for Account Id. Note "_SB" is for Sandbox account.

The `ns_consumer_key`, `ns_consumer_secret`, `ns_token_key` and `ns_token_secret` keys are your TBA Authentication keys for SOAP connection. Visit the [NetSuite documentation](https://support.cazoomi.com/hc/en-us/articles/360010093392-How-to-Setup-NetSuite-Token-Based-Authentication-as-Authentication-Type).


## The JournalEntries CSV

The csv file follows the configuration pattern below:

| Transaction Date | Journal Entry Id | Account Number | Account Name | Class | Location | Department | Customer Name | Description | Amount | Posting Type | Currency |
|------------------|------------------|----------------|--------------|-------|----------|------------|---------------|-------------|--------|--------------|----------|
| %d/%m/%y         | str              | int            | str          | str   | str      | str        | str           | str         | float  | credit/debit | 'AAA'    |


### Example
```
Transaction Date,Journal Entry Id,Account Number,Account Name,Class,Location,Department,Customer Name,Description,Amount,Posting Type,Currency
7/25/25,Jul25 Comm,2000,Deferred Commission,ENT,Texas,Marketing,,Jul25 Event,1234.56,Debit,USD
7/25/25,Jul25 Comm,4000,Contra Commission,ENT,Texas,Marketing,,Jul25 Event,1234.56,Credit,USD
```

## Run Target

To run the target, execute it with the config file.

```
> target-netsuite --config config.json
```
