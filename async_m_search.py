from elasticsearch.exceptions import TransportError

from .es_response import Response
from .es_request  import Request

class AsyncMultiSearch(Request):
    def __init__(self, **kwargs):
        super(AsyncMultiSearch, self).__init__(**kwargs)
        self._searches  = []

    def __getitem__(self, key):
        return self._searches[key]

    def __iter__(self):
        return iter(self._searches)

    def update_from_dict(self, query_dict):
        self.__setattr__('query', query_dict)

    def params(self, **kwargs):
        self._params.update(kwargs)
        return self

    def add(self, search):
        self._searches.append(search)

    def to_dict(self):
        out = []
        for s in self._searches:
            meta = {}
            if s._index:
                meta['index'] = s._index
            if s._doc_type:
                meta['type'] = s._get_doc_type()
            meta.update(s._params)

            out.append(meta)
            out.append(s.to_dict())

        return out

    async def execute(self, ignore_cache = False, raise_on_error = True):
        if ignore_cache or not hasattr(self, '_response'):
            responses = await self._using.msearch(index = self._index, doc_type = self._doc_type, body = self.to_dict(), **self._params)
            out = []
            for search, response in zip(self._searches, responses['responses']):
                if response.get('error', False):
                    if raise_on_error:
                        raise TransportError('N/A', response['error']['type'], response['error'])
                    response = None
                else:
                    response = Response(search, response)
                out.append(response)

            self._response = out

        return self._response
