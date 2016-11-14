# -*- coding: utf-8 -*-
from __future__ import absolute_import
import logging
from traceback import format_stack
from signal import signal, SIGUSR1
from logging.config import fileConfig
from argparse import ArgumentParser
from time import asctime
from os.path import exists

from twisted.internet import reactor, task
from frontera.core.components import DistributedBackend
from frontera.core.manager import FrontierManager
from frontera.utils.url import parse_domain_from_url_fast
from frontera.logger.handlers import CONSOLE

from frontera.settings import Settings
from frontera.utils.misc import load_object
from frontera.utils.async import CallLaterOnce
from .server import WorkerJsonRpcService
import six
from six.moves import map

logger = logging.getLogger("db-worker")


class Slot(object):
    def __init__(self, new_batch, consume_incoming, consume_scoring, no_batches, no_scoring_log,
                 new_batch_delay, no_spider_log):
        self.new_batch = CallLaterOnce(new_batch)
        self.new_batch.setErrback(self.error)

        self.consumption = CallLaterOnce(consume_incoming)
        self.consumption.setErrback(self.error)

        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)

        self.scoring_consumption = CallLaterOnce(consume_scoring)
        self.scoring_consumption.setErrback(self.error)

        self.no_batches = no_batches
        self.no_scoring_log = no_scoring_log
        self.no_spider_log = no_spider_log
        self.new_batch_delay = new_batch_delay

    def error(self, f):
        logger.exception(f.value)
        return f

    def schedule(self, on_start=False):
        if on_start and not self.no_batches:
            self.new_batch.schedule(0)

        if not self.no_spider_log:
            self.consumption.schedule()
        if not self.no_batches:
            self.new_batch.schedule(self.new_batch_delay)
        if not self.no_scoring_log:
            self.scoring_consumption.schedule()
        self.scheduling.schedule(5.0)


class DBWorker(object):
    def __init__(self, settings, no_batches, no_incoming, no_scoring):
        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.mb = messagebus(settings)
        spider_log = self.mb.spider_log()

        self.spider_feed = self.mb.spider_feed()
        self.spider_log_consumer = spider_log.consumer(partition_id=None, type=b'db')
        self.spider_feed_producer = self.spider_feed.producer()

        self._manager = FrontierManager.from_settings(settings, db_worker=True)
        self._backend = self._manager.backend
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        self._encoder = encoder_cls(self._manager.request_model)
        self._decoder = decoder_cls(self._manager.request_model, self._manager.response_model)

        if isinstance(self._backend, DistributedBackend) and not no_scoring:
            scoring_log = self.mb.scoring_log()
            self.scoring_log_consumer = scoring_log.consumer()
            self.queue = self._backend.queue
            self.strategy_disabled = False
        else:
            self.strategy_disabled = True
        self.spider_log_consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')
        self.scoring_log_consumer_batch_size = settings.get('SCORING_LOG_CONSUMER_BATCH_SIZE')
        self.spider_feed_partitioning = 'fingerprint' if not settings.get('QUEUE_HOSTNAME_PARTITIONING') else 'hostname'
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.slot = Slot(self.new_batch, self.consume_incoming, self.consume_scoring, no_batches,
                         self.strategy_disabled, settings.get('NEW_BATCH_DELAY'), no_incoming)
        self.job_id = 0
        self.stats = {
            'consumed_since_start': 0,
            'consumed_scoring_since_start': 0,
            'pushed_since_start': 0
        }
        self._logging_task = task.LoopingCall(self.log_status)

    def set_process_info(self, process_info):
        self.process_info = process_info

    def run(self):
        def debug(sig, frame):
            logger.critical("Signal received: printing stack trace")
            logger.critical(str("").join(format_stack(frame)))

        self.slot.schedule(on_start=True)
        self._logging_task.start(30)
        signal(SIGUSR1, debug)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        reactor.run()

    def stop(self):
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def log_status(self):
        for k, v in six.iteritems(self.stats):
            logger.info("%s=%s", k, v)

    def disable_new_batches(self):
        self.slot.no_batches = True

    def enable_new_batches(self):
        self.slot.no_batches = False

    def consume_incoming(self, *args, **kwargs):
        consumed = 0
        for m in self.spider_log_consumer.get_messages(timeout=1.0, count=self.spider_log_consumer_batch_size):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error: %s", e)
                continue
            else:
                type = msg[0]
                if type == 'add_seeds':
                    _, seeds = msg
                    logger.info('Adding %i seeds', len(seeds))
                    for seed in seeds:
                        logger.debug('URL: %s', seed.url)
                    self._backend.add_seeds(seeds)
                    continue
                if type == 'page_crawled':
                    _, response = msg
                    logger.debug("Page crawled %s", response.url)
                    if b'jid' not in response.meta or response.meta[b'jid'] != self.job_id:
                        continue
                    self._backend.page_crawled(response)
                    continue
                if type == 'links_extracted':
                    _, request, links = msg
                    logger.debug("Links extracted %s (%d)", request.url, len(links))
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self._backend.links_extracted(request, links)
                    continue
                if type == 'request_error':
                    _, request, error = msg
                    logger.debug("Request error %s", request.url)
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self._backend.request_error(request, error)
                    continue
                if type == 'offset':
                    _, partition_id, offset = msg
                    producer_offset = self.spider_feed_producer.get_offset(partition_id)
                    if producer_offset is None:
                        continue
                    else:
                        lag = producer_offset - offset
                        if lag < 0:
                            # non-sense in general, happens when SW is restarted and not synced yet with Spiders.
                            continue
                        if lag < self.max_next_requests or offset == 0:
                            self.spider_feed.mark_ready(partition_id)
                        else:
                            self.spider_feed.mark_busy(partition_id)
                    continue
                logger.debug('Unknown message type %s', type)
            finally:
                consumed += 1
        """
        # TODO: Think how it should be implemented in DB-worker only mode.
        if not self.strategy_disabled and self._backend.finished():
            logger.info("Crawling is finished.")
            reactor.stop()
        """
        self.stats['consumed_since_start'] += consumed
        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        self.slot.schedule()
        return consumed

    def consume_scoring(self, *args, **kwargs):
        consumed = 0
        seen = set()
        batch = []
        for m in self.scoring_log_consumer.get_messages(count=self.scoring_log_consumer_batch_size):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error: %s", e)
                continue
            else:
                if msg[0] == 'update_score':
                    _, request, score, schedule = msg
                    if request.meta[b'fingerprint'] not in seen:
                        batch.append((request.meta[b'fingerprint'], score, request, schedule))
                        seen.add(request.meta[b'fingerprint'])
                if msg[0] == 'new_job_id':
                    self.job_id = msg[1]
            finally:
                consumed += 1
        self.queue.schedule(batch)

        self.stats['consumed_scoring_since_start'] += consumed
        self.stats['last_consumed_scoring'] = consumed
        self.stats['last_consumption_run_scoring'] = asctime()
        self.slot.schedule()

    def new_batch(self, *args, **kwargs):
        def get_hostname(request):
            try:
                netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url_fast(request.url)
            except Exception as e:
                logger.error("URL parsing error %s, fingerprint %s, url %s" % (e, request.meta[b'fingerprint'],
                                                                               request.url))
                return None
            else:
                return name.encode('utf-8', 'ignore')

        def get_fingerprint(request):
            return request.meta[b'fingerprint']

        partitions = self.spider_feed.available_partitions()
        logger.info("Getting new batches for partitions %s" % str(",").join(map(str, partitions)))
        if not partitions:
            return 0

        count = 0
        if self.spider_feed_partitioning == 'hostname':
            get_key = get_hostname
        elif self.spider_feed_partitioning == 'fingerprint':
            get_key = get_fingerprint
        else:
            raise Exception("Unexpected value in self.spider_feed_partitioning")

        for request in self._backend.get_next_requests(self.max_next_requests, partitions=partitions):
            try:
                request.meta[b'jid'] = self.job_id
                eo = self._encoder.encode_request(request)
            except Exception as e:
                logger.error("Encoding error, %s, fingerprint: %s, url: %s" % (e,
                                                                               request.meta[b'fingerprint'],
                                                                               request.url))
                continue
            finally:
                count += 1
            self.spider_feed_producer.send(get_key(request), eo)

        self.stats['pushed_since_start'] += count
        self.stats['last_batch_size'] = count
        self.stats.setdefault('batches_after_start', 0)
        self.stats['batches_after_start'] += 1
        self.stats['last_batch_generated'] = asctime()
        return count


if __name__ == '__main__':
    parser = ArgumentParser(description="Frontera DB worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables generation of new batches.')
    parser.add_argument('--no-incoming', action='store_true',
                        help='Disables spider log processing.')
    parser.add_argument('--no-scoring', action='store_true',
                        help='Disables scoring log processing.')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import.')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL.")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen.")
    args = parser.parse_args()

    settings = Settings(module=args.config)
    if args.port:
        settings.set("JSONRPC_PORT", [args.port])

    logging_config_path = settings.get("LOGGING_CONFIG")
    if logging_config_path and exists(logging_config_path):
        fileConfig(logging_config_path)
    else:
        logging.basicConfig(level=args.log_level)
        logger.setLevel(args.log_level)
        logger.addHandler(CONSOLE)

    worker = DBWorker(settings, args.no_batches, args.no_incoming, args.no_scoring)
    server = WorkerJsonRpcService(worker, settings)
    server.start_listening()
    worker.run()

