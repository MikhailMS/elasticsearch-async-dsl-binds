import logging

from elasticsearch.exceptions import ElasticsearchException

from .es_response        import AggResponse, Response
from .es_request         import Request

class AsyncSearch(Request):
    def __init__(self, **kwargs):
        super(AsyncSearch, self).__init__(**kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_from_dict(self, query_dict):
        self.__setattr__('query', query_dict)
        return self

    def to_dict(self, count = False, **kwargs) -> dict:
        # In Elasticsearch-dsl source code this function got checks and some extras, but we don't need them for now
        # This method is needed to keep the rest of the code as close as possible to the original Elasticsearch-dsl
        return self.query

    def count(self) -> int:
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        if hasattr(self, '_response'):
            return len(self._response.hits)
        return -1

    async def execute(self, ignore_cache = False) -> Response:
        if ignore_cache or not hasattr(self, '_response'):
            self._response = Response(self, await self._using.search(index = self._index, doc_type = self._doc_type,
                                                                     body = self.to_dict(), **self._params))
        return self._response

    async def execute_agg(self, ignore_cache = False) -> AggResponse:
        if ignore_cache or not hasattr(self, '_response'):
            self._response = AggResponse(self, await self._using.search(index = self._index, doc_type = self._doc_type,
                                                                        body = self.to_dict(), **self._params))
        return self._response

    async def scan(self, scroll = '5m', bulk_return = True, request_timeout=None, clear_scroll = True, raise_on_error = False):
        # Initial call to ES
        response = await self._using.search(index = self._index, doc_type = self._doc_type,
                                            body = self.to_dict(), scroll = scroll, **self._params)
        self._scroll_id = response.get('_scroll_id')
        if self._scroll_id is None:
            return

        try:
            first_run = True
            while True:
                if first_run:
                    first_run = False
                else:
                    response = await self._using.scroll(self._scroll_id, scroll = scroll, request_timeout = request_timeout)

                if bulk_return:
                    yield Response(self, response)
                else:
                    for hit in response['hits']['hits']:
                        yield hit['_source']

                # check if we have any errrors
                if response["_shards"]["successful"] < response["_shards"]["total"]:
                    self.logger.warning('Scroll request has only succeeded on {} shards out of {}'.format(response['_shards']['successful'],
                                                                                                           response['_shards']['total']))
                if raise_on_error:
                    raise ScanError(self._scroll_id,
                                    'Scroll request has only succeeded on {} shards out of {}'.format(response['_shards']['successful'],
                                                                                                       response['_shards']['total']))

                self._scroll_id = response.get('_scroll_id')

                # end of scroll
                if self._scroll_id is None or not response['hits']['hits']:
                    break

        finally:
            await self.close_scroll(clear_scroll = clear_scroll)

    async def close_scroll(self, clear_scroll = True):
        if self._scroll_id and clear_scroll:
            await self._using.clear_scroll(body={'scroll_id': [self._scroll_id]}, ignore=(404, ))


class ScanError(ElasticsearchException):
    def __init__(self, scroll_id, *args, **kwargs):
        super(ScanError, self).__init__(*args, **kwargs)
        self.scroll_id = scroll_id
