# -*- coding: utf-8 -*-
from __future__ import absolute_import

from time import asctime
from collections import defaultdict

from frontera.exceptions import NotConfigured
from . import DBWorkerPeriodicComponent


class IncomingConsumer(DBWorkerPeriodicComponent):
    """Component to get data from spider log and handle it with backend."""

    NAME = 'incoming'

    def __init__(self, worker, settings, stop_event, no_incoming=False, **kwargs):
        super(IncomingConsumer, self).__init__(worker, settings, stop_event, **kwargs)
        if no_incoming:
            raise NotConfigured('IncomingConsumer is disabled with --no-incoming')

        spider_log = worker.message_bus.spider_log()
        self.spider_log_consumer = spider_log.consumer(partition_id=None, type=b'db')
        self.spider_log_consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')

        # spider-feed is required only to handle 'offset' messages:
        # check lag to decide if mark feed producer as busy or ready
        # XXX not implemented for kafka message bus
        self.spider_feed = worker.message_bus.spider_feed()
        self.spider_feed_producer = self.spider_feed.producer()

        self.backend = worker.backend
        self.max_next_requests = settings.MAX_NEXT_REQUESTS

    def run(self):
        consumed, stats = 0, defaultdict(int)
        for m in self.spider_log_consumer.get_messages(
                timeout=1.0, count=self.spider_log_consumer_batch_size):
            try:
                msg = self.worker._decoder.decode(m)
            except (KeyError, TypeError) as e:
                self.logger.error("Decoding error: %s", e)
            else:
                self._handle_message(msg, stats)
            finally:
                consumed += 1
        """
        # TODO: Think how it should be implemented in DB-worker only mode.
        if not self.strategy_disabled and self._backend.finished():
            logger.info("Crawling is finished.")
            reactor.stop()
        """
        stats_increments = {'consumed_since_start': consumed}
        stats_increments.update(stats)
        self.worker.update_stats(increments=stats_increments,
                                 replacements={'last_consumed': consumed,
                                               'last_consumption_run': asctime()})

    def _handle_message(self, msg, stats):
        """Base logic to safely handle a message."""
        try:
            self._handle_message_by_type(msg[0], msg, stats)
        except Exception:
            self.logger.exception("Error while handling a message")
            self.logger.debug("Message caused the error %s", str(msg))

    def _handle_message_by_type(self, msg_type, msg, stats):
        if msg_type == 'add_seeds':
            _, seeds = msg
            self.logger.info('Adding %i seeds', len(seeds))
            for seed in seeds:
                self.logger.debug('URL: %s', seed.url)
            self.backend.add_seeds(seeds)
            stats['consumed_add_seeds'] += 1

        elif msg_type == 'page_crawled':
            _, response = msg
            self.logger.debug("Page crawled %s", response.url)
            if b'jid' not in response.meta or response.meta[b'jid'] != self.worker.job_id:
                return
            self.backend.page_crawled(response)
            stats['consumed_page_crawled'] += 1

        elif msg_type == 'links_extracted':
            _, request, links = msg
            self.logger.debug("Links extracted %s (%d)", request.url, len(links))
            if b'jid' not in request.meta or request.meta[b'jid'] != self.worker.job_id:
                return
            self.backend.links_extracted(request, links)
            stats['consumed_links_extracted'] += 1

        elif msg_type == 'request_error':
            _, request, error = msg
            self.logger.debug("Request error %s", request.url)
            if b'jid' not in request.meta or request.meta[b'jid'] != self.worker.job_id:
                return
            self.backend.request_error(request, error)
            stats['consumed_request_error'] += 1

        elif msg_type == 'offset':
            _, partition_id, offset = msg
            producer_offset = self.spider_feed_producer.get_offset(partition_id)
            if producer_offset is None:
                return
            else:
                lag = producer_offset - offset
                if lag < 0:
                    # non-sense in general, happens when SW is restarted and
                    # not synced yet with Spiders.
                    return
                if lag < self.max_next_requests or offset == 0:
                    self.spider_feed.mark_ready(partition_id)
                else:
                    self.spider_feed.mark_busy(partition_id)
            stats['consumed_offset'] += 1

        else:
            self.logger.debug('Unknown message type %s', msg[0])

    def close(self):
        self.spider_feed_producer.close()
        self.spider_log_consumer.close()
