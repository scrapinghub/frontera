from __future__ import absolute_import


from binascii import unhexlify

import six

from w3lib.util import to_bytes

from frontera.core.components import Metadata

from .utils import prepare_hbase_object, utcnow_timestamp


class HBaseMetadata(Metadata):
    def __init__(
            self, connection, table_name, drop_all_tables, use_snappy,
            batch_size, store_content,
    ):
        self._table_name = to_bytes(table_name)
        tables = set(connection.tables())

        if drop_all_tables and self._table_name in tables:
            connection.delete_table(self._table_name, disable=True)
            tables.remove(self._table_name)

        if self._table_name not in tables:
            schema = {
                'm': {
                    'max_versions': 1,
                },
                's': {
                    'max_versions': 1,
                    'block_cache_enabled': 1,
                    'bloom_filter_type': 'ROW',
                    'in_memory': True,
                },
                'c': {
                    'max_versions': 1,
                },
            }

            if use_snappy:
                schema['m']['compression'] = 'SNAPPY'
                schema['c']['compression'] = 'SNAPPY'

            connection.create_table(self._table_name, schema)

        table = connection.table(self._table_name)
        self.batch = table.batch(batch_size=batch_size)
        self.store_content = store_content

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.flush()

    def flush(self):
        self.batch.send()

    def add_seeds(self, seeds):
        for seed in seeds:
            obj = prepare_hbase_object(
                url=seed.url,
                depth=0,
                created_at=utcnow_timestamp(),
                domain_fingerprint=seed.meta[b'domain'][b'fingerprint'],
            )

            self.batch.put(unhexlify(seed.meta[b'fingerprint']), obj)

    def page_crawled(self, response):
        if self.store_content:
            obj = prepare_hbase_object(
                status_code=response.status_code,
                content=response.body,
            )
        else:
            obj = prepare_hbase_object(status_code=response.status_code)

        self.batch.put(unhexlify(response.meta[b'fingerprint']), obj)

    def links_extracted(self, request, links):
        links_dict = dict()

        for link in links:
            links_dict[unhexlify(link.meta[b'fingerprint'])] = (link, link.url, link.meta[b'domain'])

        for link_fingerprint, (link, link_url, link_domain) in six.iteritems(links_dict):
            obj = prepare_hbase_object(
                url=link_url,
                created_at=utcnow_timestamp(),
                domain_fingerprint=link_domain[b'fingerprint'],
            )

            self.batch.put(link_fingerprint, obj)

    def request_error(self, request, error):
        obj = prepare_hbase_object(
            url=request.url,
            created_at=utcnow_timestamp(),
            error=error,
            domain_fingerprint=request.meta[b'domain'][b'fingerprint'],
        )
        rk = unhexlify(request.meta[b'fingerprint'])

        self.batch.put(rk, obj)

    def update_score(self, batch):
        if not isinstance(batch, dict):
            raise TypeError(
                'batch should be dict with fingerprint as key, and float '
                'score as value'
            )

        for fprint, (score, url, schedule) in six.iteritems(batch):
            obj = prepare_hbase_object(score=score)
            rk = unhexlify(fprint)

            self.batch.put(rk, obj)
