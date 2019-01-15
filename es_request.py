import collections

class Request(object):

    def __init__(self, using='default', index=None, doc_type=None, extra=None):
        self._using = using

        self._index = None
        if isinstance(index, list):
            self._index = index
        elif index:
            self._index = [index]

        self._doc_type = []
        self._doc_type_map = {}
        if isinstance(doc_type, (tuple, list)):
            self._doc_type.extend(doc_type)
        elif isinstance(doc_type, collections.Mapping):
            self._doc_type.extend(doc_type.keys())
            self._doc_type_map.update(doc_type)
        elif doc_type:
            self._doc_type.append(doc_type)

        self._params = {}
        self._extra = extra or {}

    def params(self, **kwargs):
        self._params.update(kwargs)
        return self

    def _get_doc_type(self):
        return list(set(dt._doc_type.name if hasattr(dt, '_doc_type') else dt for dt in self._doc_type))
