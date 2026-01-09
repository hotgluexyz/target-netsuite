from netsuitesdk.api.currencies import Currencies

import time
import json
import singer
from .transaction_entities import (
    Customers,
    JournalEntries,
    Locations,
    Departments,
    Accounts,
    Classifications,
    Subsidiaries,
    Items,
    TaxCodes,
    TaxAccounts,
    TaxTypes,
    TaxGroups
)
from .netsuite_client import ExtendedNetSuiteClient

LOGGER = singer.get_logger()


class ExtendedNetSuiteConnection:
    def __init__(self, account, consumer_key, consumer_secret, token_key, token_secret, caching=True):
        # NetSuiteConnection.__init__(self, account, consumer_key, consumer_secret, token_key, token_secret)
        # ns_client: NetSuiteClient = self.client

        self.client = ExtendedNetSuiteClient(account=account, caching=caching)
        self.client.connect_tba(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            token_key=token_key,
            token_secret=token_secret
        )

        self.entities = {
            'Customer': Customers,
            'Accounts': Accounts,
            'JournalEntry': JournalEntries,
            'Classifications': Classifications,
            'Currencies': Currencies,
            'Locations': Locations,
            'Departments': Departments,
            'Subsidiaries': Subsidiaries,
            'Items': Items,
            'TaxCodes': TaxCodes,
            'TaxAccounts': TaxAccounts,
            'TaxTypes': TaxTypes,
            'TaxGroups': TaxGroups
        }

    def _query_entity(self, data, entity, stream):
        to_get_results_for = data.get(stream)
        for element in to_get_results_for:
            start_time = time.time()
            internal_id = element.get('internalId')
            LOGGER.info(f"fetching data for internalId {internal_id}")
            to_return = entity.get(internalId=internal_id)
            LOGGER.info(f"Successfully fetched data for internalId {internal_id} --- %s seconds ---" % (
                        time.time() - start_time))
            yield to_return

    def query_entity(self, stream=None, lastModifiedDate=None):
        start_time = time.time()
        LOGGER.info(f"Starting fetch data for stream {stream}")
        entity = self.entities[stream]

        if hasattr(entity, 'require_lastModified_date') and entity.require_lastModified_date is True:
            data = entity.get_all(lastModifiedDate)
        else:
            data = entity.get_all()

        if hasattr(entity, 'require_paging') and entity.require_paging is True:
            transformed_data = json.dumps({stream: data}, default=str, indent=2)
            data = json.loads(transformed_data)
            to_return = list(self._query_entity(data, entity, stream))
        else:
            to_return = data

        LOGGER.info(f"Successfully fetched data for stream {stream}")
        LOGGER.info("--- %s seconds ---" % (time.time() - start_time))

        return to_return
