from __future__ import absolute_import

import logging

from time import time

from six.moves import range

from sqlalchemy import BigInteger, Column

from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Queue as BaseQueue, States
from frontera.core.models import Request
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast

from ..models import DeclarativeBase, QueueModelMixin
from ..utils import retry_and_rollback, utcnow_timestamp


logger = logging.getLogger(__name__)


class RevisitingQueueModel(QueueModelMixin, DeclarativeBase):
    __tablename__ = 'revisiting_queue'

    crawl_at = Column(BigInteger, nullable=False)


class RevisitingQueue(BaseQueue):
    def __init__(self, session_cls, queue_cls, partitions):
        self.session = session_cls()
        self.queue_model = queue_cls
        self.partitions = list(range(0, partitions))
        self.partitioner = Crc32NamePartitioner(self.partitions)

    def frontier_stop(self):
        self.session.close()

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        results = []

        try:
            query = self.session.query(self.queue_model).filter(
                RevisitingQueueModel.crawl_at <= utcnow_timestamp(),
                RevisitingQueueModel.partition_id == partition_id,
            )

            for item in query.limit(max_n_requests):

                method = item.method or 'GET'

                results.append(Request(
                    item.url, method=method, meta=item.meta,
                    headers=item.headers, cookies=item.cookies,
                ))
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

                if hostname:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                    host_crc32 = get_crc32(hostname)
                else:
                    logger.error(
                        "Can't get hostname for URL %s, fingerprint %s",
                        request.url, fprint,
                    )

                    partition_id = self.partitions[0]
                    host_crc32 = 0

                if b'crawl_at' in request.meta:
                    schedule_at = request.meta[b'crawl_at']
                else:
                    schedule_at = utcnow_timestamp()

                q = self.queue_model(
                    fingerprint=fprint, score=score, url=request.url,
                    meta=request.meta, headers=request.headers,
                    cookies=request.cookies, method=request.method,
                    partition_id=partition_id, host_crc32=host_crc32,
                    created_at=time() * 1E+6, crawl_at=schedule_at,
                )

                to_save.append(q)

                request.meta[b'state'] = States.QUEUED

        self.session.bulk_save_objects(to_save)
        self.session.commit()

    @retry_and_rollback
    def count(self):
        return self.session.query(self.queue_model).count()
