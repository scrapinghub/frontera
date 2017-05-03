from __future__ import absolute_import

from datetime import timedelta

from frontera.core.components import States

from .queue import RevisitingQueue, RevisitingQueueModel

from ..backend import SQLAlchemyBackend
from ..utils import utcnow_timestamp


class Backend(SQLAlchemyBackend):

    def _create_queue(self, settings):
        self.interval = settings.get("SQLALCHEMYBACKEND_REVISIT_INTERVAL")

        assert isinstance(self.interval, timedelta)

        self.interval = self.interval.total_seconds()

        return RevisitingQueue(
            self.session_cls,
            RevisitingQueueModel,
            settings.get('SPIDER_FEED_PARTITIONS'),
        )

    def _schedule(self, requests):
        batch = []

        for request in requests:
            if request.meta[b'state'] in [States.NOT_CRAWLED]:
                request.meta[b'crawl_at'] = utcnow_timestamp()
            elif request.meta[b'state'] in [States.CRAWLED, States.ERROR]:
                request.meta[b'crawl_at'] = utcnow_timestamp() + self.interval
            else:
                continue  # QUEUED

            batch.append((request.meta[b'fingerprint'], self._get_score(request), request, True))

        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += len(batch)

    def page_crawled(self, response):
        super(Backend, self).page_crawled(response)

        self.states.set_states(response.request)
        self._schedule([response.request])
        self.states.update_cache(response.request)
