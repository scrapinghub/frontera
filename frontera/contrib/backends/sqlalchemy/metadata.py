from __future__ import absolute_import

from datetime import datetime

from cachetools import LRUCache

from w3lib.util import to_bytes, to_native_str

from frontera.core.components import Metadata as BaseMetadata
from frontera.core.models import Request, Response

from .models import DeclarativeBase
from .utils import retry_and_rollback


class Metadata(BaseMetadata):
    def __init__(self, session_cls, model_cls, cache_size):
        # FIXME: Should be explicitly mentioned in docs
        self.session = session_cls(expire_on_commit=False)
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['metadata']
        self.cache = LRUCache(cache_size)

    def frontier_stop(self):
        self.session.close()

    @retry_and_rollback
    def add_seeds(self, seeds):
        for seed in seeds:
            o = self._create_page(seed)
            self.cache[to_bytes(o.fingerprint)] = self.session.merge(o)

        self.session.commit()

    @retry_and_rollback
    def request_error(self, page, error):
        if page.meta[b'fingerprint'] in self.cache:
            m = self._modify_page(page)
        else:
            m = self._create_page(page)

        m.error = error
        self.cache[to_bytes(m.fingerprint)] = self.session.merge(m)

        self.session.commit()

    @retry_and_rollback
    def page_crawled(self, response):
        if response.meta[b'fingerprint'] in self.cache:
            r = self._modify_page(response)
        else:
            r = self._create_page(response)

        self.cache[r.fingerprint] = self.session.merge(r)

        self.session.commit()

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'fingerprint'] not in self.cache:
                self.cache[link.meta[b'fingerprint']] = self.session.merge(self._create_page(link))

        self.session.commit()

    def _modify_page(self, obj):
        db_page = self.cache[obj.meta[b'fingerprint']]
        db_page.fetched_at = datetime.utcnow()

        if isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = to_native_str(obj.request.method)
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code

        return db_page

    def _create_page(self, obj):
        db_page = self.model()
        db_page.fingerprint = to_native_str(obj.meta[b'fingerprint'])
        db_page.url = obj.url
        db_page.created_at = datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = to_native_str(obj.method)
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = to_native_str(obj.request.method)
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code

        return db_page

    @retry_and_rollback
    def update_score(self, batch):
        for fprint, score, request, schedule in batch:
            m = self.model(fingerprint=to_native_str(fprint), score=score)

            self.session.merge(m)

        self.session.commit()
