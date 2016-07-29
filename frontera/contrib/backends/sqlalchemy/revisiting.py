# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
from datetime import datetime, timedelta
from time import time, sleep
from calendar import timegm

from sqlalchemy import Column, BigInteger

from frontera import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.sqlalchemy import SQLAlchemyBackend
from frontera.contrib.backends.sqlalchemy.models import QueueModelMixin, DeclarativeBase
from frontera.core.components import Queue as BaseQueue, States
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast
from six.moves import range


def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())


class RevisitingQueueModel(QueueModelMixin, DeclarativeBase):
    __tablename__ = 'revisiting_queue'

    crawl_at = Column(BigInteger, nullable=False)


def retry_and_rollback(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception as exc:
                self.logger.exception(exc)
                self.session.rollback()
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
        self.logger = logging.getLogger("sqlalchemy.revisiting.queue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)

    def frontier_stop(self):
        self.session.close()

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        results = []
        try:
            for item in self.session.query(self.queue_model).\
                    filter(RevisitingQueueModel.crawl_at <= utcnow_timestamp(),
                           RevisitingQueueModel.partition_id == partition_id).\
                    limit(max_n_requests):
                method = 'GET' if not item.method else item.method
                results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers,
                                       cookies=item.cookies))
                self.session.delete(item)
            self.session.commit()
        except Exception as exc:
            self.logger.exception(exc)
            self.session.rollback()
        return results

    @retry_and_rollback
    def schedule(self, batch):
        to_save = []
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
                q = self.queue_model(fingerprint=fprint, score=score, url=request.url, meta=request.meta,
                                     headers=request.headers, cookies=request.cookies, method=request.method,
                                     partition_id=partition_id, host_crc32=host_crc32, created_at=time()*1E+6,
                                     crawl_at=schedule_at)
                to_save.append(q)
                request.meta[b'state'] = States.QUEUED
        self.session.bulk_save_objects(to_save)
        self.session.commit()

    @retry_and_rollback
    def count(self):
        return self.session.query(self.queue_model).count()


class Backend(SQLAlchemyBackend):

    def _create_queue(self, settings):
        self.interval = settings.get("SQLALCHEMYBACKEND_REVISIT_INTERVAL")
        assert isinstance(self.interval, timedelta)
        self.interval = self.interval.total_seconds()
        return RevisitingQueue(self.session_cls, RevisitingQueueModel, settings.get('SPIDER_FEED_PARTITIONS'))

    def _schedule(self, requests):
        batch = []
        for request in requests:
            if request.meta[b'state'] in [States.NOT_CRAWLED]:
                request.meta[b'crawl_at'] = utcnow_timestamp()
            elif request.meta[b'state'] in [States.CRAWLED, States.ERROR]:
                request.meta[b'crawl_at'] = utcnow_timestamp() + self.interval
            else:
                continue    # QUEUED
            batch.append((request.meta[b'fingerprint'], self._get_score(request), request, True))
        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += len(batch)

    def page_crawled(self, response):
        super(Backend, self).page_crawled(response)
        self.states.set_states(response.request)
        self._schedule([response.request])
        self.states.update_cache(response.request)
