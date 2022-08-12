from netsuitesdk.internal.client import NetSuiteClient


class ExtendedNetSuiteClient(NetSuiteClient):
    def __init__(self, account=None, caching=True, caching_timeout=2592000):
        NetSuiteClient.__init__(self, account, caching, caching_timeout)
        self._search_preferences = self.SearchPreferences(
            bodyFieldsOnly=True,
            pageSize=1000,
            returnSearchColumns=True
        )
