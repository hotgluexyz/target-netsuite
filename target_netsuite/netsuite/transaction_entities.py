from collections import OrderedDict
from netsuitesdk.internal.utils import PaginatedSearch
import backoff

from netsuitesdk.api.base import ApiBase

from zeep.exceptions import Fault

import singer
from target_netsuite.netsuite.utils import clean_logs

logger = singer.get_logger()

class BaseFilter(ApiBase):
    def get_all(self, selected_fileds=[]):
        output = []
        page_n = 1
        selected_fileds = selected_fileds + ["externalId", "internalId"]
        for page in self.get_page():
            logger.info(f"Getting {self.type_name}: page {page_n}")
            for record in page:
                record = record.__dict__["__values__"]
                rec_dict = {}
                for k, v in record.items():
                    if k in selected_fileds:
                        if getattr(v, "__dict__", None) and v.__dict__["__values__"].get("recordRef"):
                            values = v.__dict__["__values__"]["recordRef"]
                            rec_dict[k] = [dict(value.__dict__["__values__"]) for value in values]
                        else:
                            rec_dict[k] = v
                output.append(rec_dict)
            page_n +=1
        return output
    
    @backoff.on_exception(backoff.expo, (Fault, Exception), max_tries=5, factor=3)
    def get_page(self):
        for page in self.get_all_generator():
            yield page


class Customers(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='customer')
        self.require_lastModified_date = True

    def get_all_generator(self, page_size=1000, last_modified_date=None):
        search_record = self.ns_client.basic_search_factory(type_name="Customer",
                                                            lastModifiedDate=last_modified_date)
        
        ps = PaginatedSearch(client=self.ns_client, type_name='Customer', pageSize=page_size,
                             search_record=search_record)
        return self._paginated_search_generator(ps)

    def post(self, data) -> OrderedDict:
        return None


class Locations(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Location')


class Subsidiaries(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Subsidiary')


class Departments(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Department')


class Accounts(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Account')
        ns_client._search_preferences = ns_client.SearchPreferences(
                bodyFieldsOnly=False,
                pageSize=1000,
                returnSearchColumns=True
            )


class Classifications(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Classification')


class Items(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='Item')

class TaxCodes(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='SalesTaxItem')

class TaxAccounts(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='TaxAcct')

    # TaxAcct is not a searchable type; use getAll instead
    def get_page(self):
        yield self._get_all()

    def get_all(self, selected_fileds=[]):
        return self._get_all()

    def get_all_generator(self, page_size=1000, last_modified_date=None):
        for r in self._get_all_generator():
            yield r

class TaxTypes(BaseFilter):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='TaxType')
    
    def get_page(self):
        yield self._get_all()

    def get_all(self, selected_fileds=[]):
        return self._get_all()

    def get_all_generator(self, page_size=1000, last_modified_date=None):
        for r in self._get_all_generator():
            yield r

    # SuiteTax-safe helpers
    def _get_select_value_page(self, record_type, field_id, page_index=1):
        """Attempt to call NetSuite getSelectValue for a single page.

        Tries multiple client method names for compatibility across netsuitesdk versions.
        Returns a list of {internalId, name} dicts for the page or an empty list on failure.
        """
        try:
            # Use account-specific service with auth headers
            field_desc = {'recordType': record_type}
            if field_id:
                field_desc['field'] = field_id
            res = self.ns_client.request('getSelectValue', fieldDescription=field_desc, pageIndex=page_index)
            result = getattr(res, 'body', None)
            result = getattr(result, 'getSelectValueResult', result)
            base_ref_list = getattr(result, 'baseRefList', None)
            base_refs = getattr(base_ref_list, 'baseRef', None) if base_ref_list is not None else None
            values = []
            for v in base_refs or []:
                vd = getattr(v, '__dict__', {}).get('__values__') if hasattr(v, '__dict__') else v
                if isinstance(vd, dict):
                    values.append({
                        'internalId': vd.get('internalId') or vd.get('internal_id') or vd.get('value'),
                        'name': vd.get('name') or vd.get('text')
                    })
            return [x for x in values if x.get('internalId')]
        except Exception as e:
            logger.error(f"Error getting select value: {e}")
            return []

    def get_tax_types_via_select(self):
        """List all Tax Types using getSelectValue with pagination.

        Returns a list of {internalId, name}.
        """
        results = []
        page = 1
        results = []
        while True:
            page_values = self._get_select_value_page(record_type='salesTaxItem', field_id='taxType', page_index=page)
            if not page_values:
                break
            results.extend(page_values)
            page += 1
        # Deduplicate by internalId while keeping first name encountered
        unique = {}
        for r in results:
            if r.get('internalId') and r['internalId'] not in unique:
                unique[r['internalId']] = r
        return list(unique.values())

    def get_tax_accounts(self):
        """Fetch Tax Types and their linked Tax Control Accounts.

        - Uses getSelectValue to list Tax Types
        - For each Tax Type, calls get to fetch the record
        - Extracts likely account references (liability/sales and asset/purchase)
        - Optionally fetches full TaxAcct records via getList
        Returns a list of dicts per Tax Type.
        """
        types = self.get_tax_types_via_select()
        results = []
        for t in types:
            try:
                rec = self.get(internalId=t['internalId'])
            except Exception as e:
                logger.error(f"Error getting tax type: {e}")
                rec = None
                continue

            nexus_accounts_list = rec.nexusAccountsList
            if nexus_accounts_list is not None:
                nexus_accounts = nexus_accounts_list.taxTypeNexusAccounts or []
                for na in nexus_accounts:
                    if na.payablesAccount is not None:
                        results.append(na.payablesAccount)
                    if na.receivablesAccount is not None:
                        results.append(na.receivablesAccount)

        return results

class JournalEntries(ApiBase):
    def __init__(self, ns_client):
        ApiBase.__init__(self, ns_client=ns_client, type_name='journalEntry')
        self.require_lastModified_date = True
        ns_client._search_preferences = ns_client.SearchPreferences(
                bodyFieldsOnly=False,
                pageSize=1000,
                returnSearchColumns=True
            )

    def get_all(self, last_modified_date=None):
        return list(self.get_all_generator() if last_modified_date is None else self.get_all_generator(
            last_modified_date=last_modified_date))

    def get_all_generator(self, page_size=100, last_modified_date=None):
        record_type_search_field = self.ns_client.SearchStringField(searchValue='JournalEntry', operator='contains')
        basic_search = self.ns_client.basic_search_factory('Transaction',
                                                           lastModifiedDate=last_modified_date,
                                                           recordType=record_type_search_field)

        paginated_search = PaginatedSearch(client=self.ns_client,
                                           basic_search=basic_search,
                                           type_name='Transaction',
                                           pageSize=page_size)

        return self._paginated_search_to_generator(paginated_search=paginated_search)
    
    def prepare_custom_fields(self, eod):
        if 'customFieldList' in eod and eod['customFieldList']:
            custom_fields = []
            for field in eod['customFieldList']:
                if field['type'] == 'String':
                    custom_fields.append(
                        self.ns_client.StringCustomFieldRef(
                            scriptId=field['scriptId'] if 'scriptId' in field else None,
                            internalId=field['internalId'] if 'internalId' in field else None,
                            value=field['value']
                        )
                    )
                elif field['type'] == 'Select':
                    custom_fields.append(
                        self.ns_client.SelectCustomFieldRef(
                            scriptId=field['scriptId'] if 'scriptId' in field else None,
                            internalId=field['internalId'] if 'internalId' in field else None,
                            value=self.ns_client.ListOrRecordRef(
                                internalId=field['value']
                            )
                        )
                    )
            return self.ns_client.CustomFieldList(custom_fields)
        return None

    def post(self, data) -> OrderedDict:
        assert data['externalId'], 'missing external id'
        je = self.ns_client.JournalEntry(externalId=data['externalId'])
        line_list = []
        for eod in data['lineList']:
            eod['customFieldList'] = self.prepare_custom_fields(eod)
            jee = self.ns_client.JournalEntryLine(**eod)
            line_list.append(jee)

        je['lineList'] = self.ns_client.JournalEntryLineList(line=line_list)
        
        if data['currency'] is not None:
            je['currency'] = self.ns_client.RecordRef(**(data['currency']))

        if 'customFieldList' in data:
            je['customFieldList'] = data['customFieldList']
        
        if 'memo' in data:
            je['memo'] = data['memo']

        if 'tranDate' in data:
            je['tranDate'] = data['tranDate']

        if 'tranId' in data:
            je['tranId'] = data['tranId']

        if 'subsidiary' in data:
            je['subsidiary'] = data['subsidiary']

        if 'class' in data:
            je['class'] = data['class']

        if 'location' in data:
            je['location'] = data['location']

        if 'department' in data:
            je['department'] = data['department']

        logger.info(
            f"Posting JournalEntries now with {len(je['lineList']['line'])} entries. ExternalId {je['externalId']} tranDate {je['tranDate']}")
        try:
            res = self.ns_client.upsert(je)
        except Exception as e:
            je_log = clean_logs(je)
            logger.error(f"Error posting journal entry: {je_log}")
            raise e 
        
        return self._serialize(res)
