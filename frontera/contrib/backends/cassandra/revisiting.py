# -*- coding: utf-8 -*-
import logging
import json
from datetime import datetime, timedelta
from time import time, sleep

from frontera import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.cassandra import CassandraBackend
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from frontera.core.components import Queue as BaseQueue, States
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast


class RevisitingQueueModel(Model):
    __table_name__ = 'revisiting_queue'

    crawl_at = columns.DateTime(required=True, default=datetime.now(), index=True)


def retry_and_rollback(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception, exc:
                self.logger.exception(exc)
                sleep(5)
                tries -= 1
                if tries > 0:
                    self.logger.info("Tries left %i" % tries)
                    continue
                else:
                    raise exc
    return func_wrapper


class RevisitingQueue(BaseQueue):
    def __init__(self, session_cls, queue_cls, partitions):
        self.session = session_cls()
        self.queue_model = queue_cls
        self.logger = logging.getLogger("frontera.contrib.backends.sqlalchemy.revisiting.RevisitingQueue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)

    def frontier_stop(self):
        pass

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        results = []
        try:
            for item in self.queue_model.objects.filter(crawl_at=datetime.utcnow(), partition_id=partition_id).\
                    limit(max_n_requests):
                method = 'GET' if not item.method else item.method
                results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers,
                                       cookies=item.cookies))
                item.delete()
        except Exception, exc:
            self.logger.exception(exc)
        return results

    @retry_and_rollback
    def schedule(self, batch):
        for fprint, score, request, schedule_at in batch:
            if schedule_at:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (request.url, fprint))
                    partition_id = self.partitions[0]
                    host_crc32 = 0
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                    host_crc32 = get_crc32(hostname)
                created_at = time()*1E+6
                q = self._create_queue(request, fprint, score, partition_id, host_crc32, created_at)

                q.save()
                request.meta['state'] = States.QUEUED

    def _create_queue(self, obj, fingerprint, score, partition_id, host_crc32, created_at):
        db_queue = self.queue_model()
        db_queue.fingerprint = fingerprint
        db_queue.score = score
        db_queue.partition_id = partition_id
        db_queue.host_crc32 = host_crc32
        db_queue.url = obj.url
        db_queue.created_at = created_at

        new_dict = {}
        for kmeta, vmeta in obj.meta.iteritems():
            if type(vmeta) is dict:
                new_dict[kmeta] = json.dumps(vmeta)
            else:
                new_dict[kmeta] = str(vmeta)

        db_queue.meta = new_dict
        db_queue.depth = 0

        db_queue.headers = obj.headers
        db_queue.method = obj.method
        db_queue.cookies = obj.cookies

        return db_queue

    @retry_and_rollback
    def count(self):
        return self.session.query(self.queue_model).count()


class Backend(CassandraBackend):

    def _create_queue(self, settings):
        self.interval = settings.get("SQLALCHEMYBACKEND_REVISIT_INTERVAL")
        assert isinstance(self.interval, timedelta)
        return RevisitingQueue(self.session_cls, RevisitingQueueModel, settings.get('SPIDER_FEED_PARTITIONS'))

    def _schedule(self, requests):
        batch = []
        queue_incr = 0
        for request in requests:
            if request.meta['state'] in [States.NOT_CRAWLED, None]:
                schedule_at = datetime.utcnow()
            elif request.meta['state'] in [States.CRAWLED, States.ERROR]:
                schedule_at = datetime.utcnow() + self.interval
            else:  # QUEUED
                schedule_at = None
            batch.append((request.meta['fingerprint'], self._get_score(request), request, schedule_at))
            if schedule_at:
                queue_incr += 1
        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += queue_incr

    def page_crawled(self, response, links):
        super(Backend, self).page_crawled(response, links)
        self._schedule([response.request])
