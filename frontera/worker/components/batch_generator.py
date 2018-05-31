# -*- coding: utf-8 -*-
from __future__ import absolute_import

import threading
from time import asctime, time
from collections import defaultdict
from logging import DEBUG

from frontera.exceptions import NotConfigured
from frontera.utils.url import parse_domain_from_url_fast
from . import DBWorkerThreadComponent


class BatchGenerator(DBWorkerThreadComponent):
    """Component to get data from backend and send it to spider feed log."""

    NAME = 'batchgen'

    def __init__(self, worker, settings, stop_event,
                 no_batches=False, partitions=None, **kwargs):
        super(BatchGenerator, self).__init__(worker, settings, stop_event, **kwargs)
        if no_batches:
            raise NotConfigured('BatchGenerator is disabled with --no-batches')

        self.run_backoff = settings.get('NEW_BATCH_DELAY')
        self.backend = worker.backend
        self.spider_feed = worker.message_bus.spider_feed()
        self.spider_feed_producer = self.spider_feed.producer()

        self.get_key_function = self.get_fingerprint
        if settings.get('QUEUE_HOSTNAME_PARTITIONING'):
            self.get_key_function = self.get_hostname

        self.domains_blacklist = settings.get('DOMAINS_BLACKLIST')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.partitions = partitions
        # create an event to disable/enable batches generation via RPC
        self.disabled_event = threading.Event()

        # domain statistics logging
        self.domain_stats = dict([(partition_id, defaultdict(int)) for partition_id in self.partitions])
        self.domain_stats_interval = settings.get('DOMAIN_STATS_LOG_INTERVAL')
        self.rotate_time = time() + self.domain_stats_interval

    def get_ready_partitions(self):
        pending_partitions = self.spider_feed.available_partitions()
        if not self.partitions:
            return pending_partitions
        return list(set(pending_partitions) & set(self.partitions))

    def run(self):
        if self.disabled_event.is_set():
            return True
        if self.logger.isEnabledFor(DEBUG) and time() > self.rotate_time:
            self.rotate_and_log_domain_stats()

        partitions = self.get_ready_partitions()
        if not partitions:
            return True
        batch_count = sum(self._handle_partition(partition_id)
                          for partition_id in partitions)
        if not batch_count:
            return True
        # let's count full batches in the same way as before
        self.update_stats(increments={'batches_after_start': 1},
                          replacements={'last_batch_size': batch_count,
                                        'last_batch_generated': asctime()})

    def _handle_partition(self, partition_id):
        self.logger.info("Getting new batches for partition %d", partition_id)
        count = 0
        for request in self.backend.get_next_requests(self.max_next_requests,
                                                      partitions=[partition_id]):
            if self._is_domain_blacklisted(request):
                continue
            try:
                request.meta[b'jid'] = self.worker.job_id
                eo = self.worker._encoder.encode_request(request)
            except Exception as e:
                self.logger.error("Encoding error, %s, fingerprint: %s, url: %s" %
                                  (e, self.get_fingerprint(request), request.url))
                count += 1  # counts as a processed request
                continue
            try:
                self.spider_feed_producer.send(self.get_key_function(request), eo)
            except Exception:
                self.logger.exception("Sending message error fingerprint: %s, url: %s" %
                                      (self.get_fingerprint(request), request.url))
            finally:
                count += 1
                hostname = self.get_hostname(request)
                if self.logger.isEnabledFor(DEBUG):
                    self.domain_stats[partition_id][hostname] += 1
        self.update_stats(increments={'pushed_since_start': count})
        return count

    def _is_domain_blacklisted(self, request):
        if not self.domains_blacklist:
            return
        if 'domain' in request.meta:
            hostname = request.meta['domain'].get('name')
        else:
            _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
        if hostname:
            hostname = hostname.lower()
            if hostname in self.domains_blacklist:
                self.logger.debug("Dropping black-listed hostname, URL %s", request.url)
                return True
        return False

    def close(self):
        self.spider_feed_producer.close()

    def rotate_and_log_domain_stats(self):
        self.logger.debug("Domain statistics of requests pushed to spider feed")
        for partition_id, host_stats in sorted(self.domain_stats.items(), key=lambda x: x[0]):
            self.logger.debug("PID %d =================================================================", partition_id)
            for hostname, count in host_stats.items():
                self.logger.debug("%s\t%d", hostname, count)

            self.domain_stats[partition_id] = defaultdict(int)
        self.rotate_time = time() + self.domain_stats_interval

    # --------------------------- Auxiliary tools --------------------------------

    def get_fingerprint(self, request):
        return request.meta[b'fingerprint']

    def get_hostname(self, request):
        return request.meta[b'domain'][b'name']
