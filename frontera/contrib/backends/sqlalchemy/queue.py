from __future__ import absolute_import

import logging

from time import time

from w3lib.util import to_bytes, to_native_str

from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Queue as BaseQueue, States as BaseStates
from frontera.core.models import Request
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast

from .utils import retry_and_rollback


logger = logging.getLogger(__name__)


class Queue(BaseQueue):
    def __init__(self, session_cls, queue_cls, partitions, ordering='default'):
        self.session = session_cls()
        self.queue_model = queue_cls
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.ordering = ordering

    def frontier_stop(self):
        self.session.close()

    def _order_by(self, query):
        if self.ordering == 'created':
            return query.order_by(self.queue_model.created_at)

        if self.ordering == 'created_desc':
            return query.order_by(self.queue_model.created_at.desc())

        # TODO: remove second parameter, it's not necessary for proper
        #       crawling, but needed for tests
        return query.order_by(self.queue_model.score, self.queue_model.created_at)

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        :param max_n_requests: maximum number of requests to return
        :param partition_id: partition id
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        results = []

        try:
            query = self.session.query(self.queue_model).filter_by(partition_id=partition_id)

            for item in self._order_by(query).limit(max_n_requests):
                method = item.method or b'GET'
                r = Request(
                    item.url, method=method, meta=item.meta,
                    headers=item.headers, cookies=item.cookies,
                )
                r.meta[b'fingerprint'] = to_bytes(item.fingerprint)
                r.meta[b'score'] = item.score

                results.append(r)
                self.session.delete(item)

            self.session.commit()
        except Exception as exc:
            logger.exception(exc)
            self.session.rollback()

        return results

    @retry_and_rollback
    def schedule(self, batch):
        to_save = []

        for fprint, score, request, schedule in batch:
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)

                if not hostname:
                    logger.error(
                        "Can't get hostname for URL %s, fingerprint %s",
                        request.url, fprint,
                    )

                    partition_id = self.partitions[0]
                    host_crc32 = 0
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                    host_crc32 = get_crc32(hostname)

                q = self.queue_model(
                    fingerprint=to_native_str(fprint), score=score,
                    url=request.url, meta=request.meta,
                    headers=request.headers, cookies=request.cookies,
                    method=to_native_str(request.method),
                    partition_id=partition_id, host_crc32=host_crc32,
                    created_at=time() * 1E+6,
                )

                to_save.append(q)

                request.meta[b'state'] = BaseStates.QUEUED

        self.session.bulk_save_objects(to_save)
        self.session.commit()

    @retry_and_rollback
    def count(self):
        return self.session.query(self.queue_model).count()
