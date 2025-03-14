from collections import OrderedDict
from netsuitesdk.internal.utils import PaginatedSearch
import backoff

from netsuitesdk.api.base import ApiBase

from zeep.exceptions import Fault

import singer
import re

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
            logger.error(f"Error posting journal entry: {je}")

            match = re.search(r"Invalid entity reference key (\d+) for subsidiary (\d+)", e.__str__())
            if match:
                entity_id = match.group(1)
                subsidiary_id = match.group(2)
                error_message = f"Customer '{entity_id}' can not be used with subsidiary '{subsidiary_id}'"
                raise Exception(error_message)
            raise e 
        
        return self._serialize(res)
