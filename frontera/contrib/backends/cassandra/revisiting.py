# -*- coding: utf-8 -*-
import logging
import uuid
from datetime import timedelta
from time import time

from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from w3lib.util import to_native_str

from frontera import Request
from frontera.contrib.backends import CommonRevisitingStorageBackendMixin
from frontera.contrib.backends.cassandra import CassandraBackend
from frontera.contrib.backends.cassandra.models import RevisitingQueueModel
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Queue as BaseQueue
from frontera.core.components import States
from frontera.utils.misc import get_crc32, utcnow_timestamp
from frontera.utils.url import parse_domain_from_url_fast


class RevisitingQueue(BaseQueue):
    def __init__(self, queue_cls, partitions):
        self.queue_model = queue_cls
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.revisiting.RevisitingQueue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.batch = BatchQuery()
        sync_table(queue_cls)

    def frontier_stop(self):
        pass

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        results = []
        try:
            for item in self.queue_model.objects.filter(partition_id=partition_id,
                                                        crawl_at__lte=utcnow_timestamp()).limit(max_n_requests):
                method = 'GET' if not item.method else item.method
                results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers,
                                       cookies=item.cookies))
                item.batch(self.batch).delete()
                self.batch.execute()
        except Exception as exc:
            self.logger.exception(exc)
        return results

    def schedule(self, batch):
        for fprint, score, request, schedule in batch:
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (request.url, fprint))
                    partition_id = self.partitions[0]
                    host_crc32 = 0
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                    host_crc32 = get_crc32(hostname)
                schedule_at = request.meta[b'crawl_at'] if b'crawl_at' in request.meta else utcnow_timestamp()
                q = self.queue_model(id=uuid.uuid4(),
                                     fingerprint=to_native_str(fprint),
                                     score=score,
                                     url=request.url,
                                     meta=request.meta,
                                     headers=request.headers,
                                     cookies=request.cookies,
                                     method=to_native_str(request.method),
                                     partition_id=partition_id,
                                     host_crc32=host_crc32,
                                     created_at=time() * 1E+6,
                                     crawl_at=schedule_at)
                q.batch(self.batch).save()
                request.meta[b'state'] = States.QUEUED
        self.batch.execute()

    def _create_queue_obj(self, fprint, score, request, partition_id, host_crc32, schedule_at):
        q = self.queue_model(id=uuid.uuid4(),
                             fingerprint=to_native_str(fprint),
                             score=score,
                             url=request.url,
                             meta=request.meta,
                             headers=request.headers,
                             cookies=request.cookies,
                             method=to_native_str(request.method),
                             partition_id=partition_id,
                             host_crc32=host_crc32,
                             created_at=time() * 1E+6,
                             crawl_at=schedule_at)
        return q

    def count(self):
        return self.queue_model.all().count()


class Backend(CommonRevisitingStorageBackendMixin, CassandraBackend):

    def _create_queue(self, settings):
        self.interval = settings.get("CASSANDRABACKEND_REVISIT_INTERVAL")
        assert isinstance(self.interval, timedelta)
        self.interval = self.interval.total_seconds()
        return RevisitingQueue(RevisitingQueueModel, settings.get('SPIDER_FEED_PARTITIONS'))
