class Response():
    def __init__(self, search, response):
        self.__setattr__('_search', search)
        self.hits = [ entry['_source'] for entry in response['hits']['hits'] ]

    def __iter__(self):
        return iter(self.hits)

    def __getitem__(self, key):
        if isinstance(key, (slice, int)):
            # for slicing etc
            return self.hits[key]
        return super(Response, self).__getitem__(key)

    def pop(self, index):
        self.hits.pop(index)

class AggResponse():
    def __init__(self, search, response):
        try:
            self._data = { key: [ item['key'] for item in meta['buckets'] ] for key, meta in response['aggregations'].items() }
        except KeyError:
            self._data = { "error": [] }

    def __iter__(self):
        return iter(self._data.items())

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._data[key]
        raise Exception(f'{key} must be a key of type string')
