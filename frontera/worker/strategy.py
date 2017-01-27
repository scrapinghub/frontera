# -*- coding: utf-8 -*-
from __future__ import absolute_import
from time import asctime
import logging
from traceback import format_stack, format_tb
from signal import signal, SIGUSR1
from logging.config import fileConfig
from argparse import ArgumentParser
from os.path import exists
from frontera.utils.misc import load_object

from frontera.core.manager import FrontierManager
from frontera.logger.handlers import CONSOLE
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from frontera.settings import Settings
from collections import Iterable
from binascii import hexlify
import six


logger = logging.getLogger("strategy-worker")


class UpdateScoreStream(object):
    def __init__(self, encoder, scoring_log_producer, size):
        self._encoder = encoder
        self._buffer = []
        self._producer = scoring_log_producer
        self._size = size

    def send(self, request, score=1.0, dont_queue=False):
        encoded = self._encoder.encode_update_score(
            request,
            score,
            not dont_queue
        )
        self._buffer.append(encoded)
        if len(self._buffer) > self._size:
            self.flush()

    def flush(self):
        if self._buffer:
            self._producer.send(None, *self._buffer)
            self._buffer = []


class StatesContext(object):

    def __init__(self, states):
        self._requests = []
        self._states = states
        self._fingerprints = set()

    def to_fetch(self, requests):
        if isinstance(requests, Iterable):
            self._fingerprints.update(x.meta[b'fingerprint'] for x in requests)
            return
        self._fingerprints.add(requests.meta[b'fingerprint'])

    def fetch(self):
        self._states.fetch(self._fingerprints)
        self._fingerprints.clear()

    def refresh_and_keep(self, requests):
        self.to_fetch(requests)
        self.fetch()
        self._states.set_states(requests)
        self._requests.extend(requests)

    def release(self):
        self._states.update_cache(self._requests)
        self._requests = []

    def flush(self):
        logger.info("Flushing states")
        self._states.flush(force_clear=False)
        logger.info("Flushing of states finished")


class StrategyWorker(object):
    def __init__(self, settings, strategy_class):
        partition_id = settings.get('SCORING_PARTITION_ID')
        if partition_id is None or type(partition_id) != int:
            raise AttributeError("Scoring worker partition id isn't set.")

        messagebus = load_object(settings.get('MESSAGE_BUS'))
        mb = messagebus(settings)
        spider_log = mb.spider_log()
        scoring_log = mb.scoring_log()
        self.consumer = spider_log.consumer(partition_id=partition_id, type=b'sw')
        self.scoring_log_producer = scoring_log.producer()

        self._manager = FrontierManager.from_settings(settings, strategy_worker=True)
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        self._decoder = decoder_cls(self._manager.request_model, self._manager.response_model)
        self._encoder = encoder_cls(self._manager.request_model)

        self.update_score = UpdateScoreStream(self._encoder, self.scoring_log_producer, 1024)
        self.states_context = StatesContext(self._manager.backend.states)

        self.consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')
        self.strategy = strategy_class.from_worker(self._manager, self.update_score, self.states_context)
        self.states = self._manager.backend.states
        self.stats = {
            'consumed_since_start': 0
        }
        self.job_id = 0
        self.task = LoopingCall(self.work)
        self._logging_task = LoopingCall(self.log_status)
        self._flush_states_task = LoopingCall(self.flush_states)
        logger.info("Strategy worker is initialized and consuming partition %d", partition_id)

    def collect_unknown_message(self, msg):
        logger.debug('Unknown message %s', msg)

    def on_unknown_message(self, msg):
        pass

    def collect_batch(self):
        consumed = 0
        batch = []
        for m in self.consumer.get_messages(count=self.consumer_batch_size, timeout=1.0):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error:")
                logger.exception(e)
                logger.debug("Message %s", hexlify(m))
                continue
            else:
                type = msg[0]
                batch.append(msg)
                try:
                    if type == 'add_seeds':
                        _, seeds = msg
                        self.states_context.to_fetch(seeds)
                        continue
                    if type == 'page_crawled':
                        _, response = msg
                        self.states_context.to_fetch(response)
                        continue
                    if type == 'links_extracted':
                        _, request, links = msg
                        self.states_context.to_fetch(request)
                        self.states_context.to_fetch(links)
                        continue
                    if type == 'request_error':
                        _, request, error = msg
                        self.states_context.to_fetch(request)
                        continue
                    if type == 'offset':
                        continue
                    self.collect_unknown_message(msg)
                except Exception as exc:
                    logger.exception(exc)
                    pass
            finally:
                consumed += 1
        return (batch, consumed)

    def process_batch(self, batch):
        for msg in batch:
            type = msg[0]
            try:
                if type == 'add_seeds':
                    _, seeds = msg
                    for seed in seeds:
                        seed.meta[b'jid'] = self.job_id
                    self.on_add_seeds(seeds)
                    continue
                if type == 'page_crawled':
                    _, response = msg
                    if b'jid' not in response.meta or response.meta[b'jid'] != self.job_id:
                        continue
                    self.on_page_crawled(response)
                    continue
                if type == 'links_extracted':
                    _, request, links = msg
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self.on_links_extracted(request, links)
                    continue
                if type == 'request_error':
                    _, request, error = msg
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self.on_request_error(request, error)
                    continue
                self.on_unknown_message(msg)
            except Exception as exc:
                logger.exception(exc)
                pass

    def work(self):
        batch, consumed = self.collect_batch()
        self.states_context.fetch()
        self.process_batch(batch)
        self.update_score.flush()
        self.states_context.release()

        # Exiting, if crawl is finished
        if self.strategy.finished():
            logger.info("Successfully reached the crawling goal.")
            logger.info("Closing crawling strategy.")
            self.strategy.close()
            logger.info("Finishing.")
            reactor.callFromThread(reactor.stop)

        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        self.stats['consumed_since_start'] += consumed

    def run(self):
        def log_failure(failure):
            logger.exception(failure.value)
            if failure.frames:
                logger.critical(str("").join(format_tb(failure.getTracebackObject())))

        def errback_main(failure):
            log_failure(failure)
            self.task.start(interval=0).addErrback(errback_main)

        def errback_flush_states(failure):
            log_failure(failure)
            self._flush_states_task.start(interval=300).addErrback(errback_flush_states)

        def debug(sig, frame):
            logger.critical("Signal received: printing stack trace")
            logger.critical(str("").join(format_stack(frame)))

        self.task.start(interval=0).addErrback(errback_main)
        self._logging_task.start(interval=30)
        self._flush_states_task.start(interval=300).addErrback(errback_flush_states)
        signal(SIGUSR1, debug)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        reactor.run()

    def log_status(self):
        for k, v in six.iteritems(self.stats):
            logger.info("%s=%s", k, v)

    def flush_states(self):
        self.states_context.flush()

    def stop(self):
        logger.info("Closing crawling strategy.")
        self.strategy.close()
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def on_add_seeds(self, seeds):
        logger.debug('Adding %i seeds', len(seeds))
        for seed in seeds:
            logger.debug("URL: %s", seed.url)
        self.states.set_states(seeds)
        self.strategy.add_seeds(seeds)
        self.states.update_cache(seeds)

    def on_page_crawled(self, response):
        logger.debug("Page crawled %s", response.url)
        self.states.set_states([response])
        self.strategy.page_crawled(response)
        self.states.update_cache(response)

    def on_links_extracted(self, request, links):
        logger.debug("Links extracted %s (%d)", request.url, len(links))
        for link in links:
            logger.debug("URL: %s", link.url)
        self.states.set_states(links)
        self.strategy.links_extracted(request, links)
        self.states.update_cache(links)

    def on_request_error(self, request, error):
        logger.debug("Page error %s (%s)", request.url, error)
        self.states.set_states(request)
        self.strategy.page_error(request, error)
        self.states.update_cache(request)


def setup_environment():
    parser = ArgumentParser(description="Frontera strategy worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--strategy', type=str,
                        help='Crawling strategy class path')
    parser.add_argument('--partition-id', type=int,
                        help="Instance partition id.")
    args = parser.parse_args()
    settings = Settings(module=args.config)
    strategy_classpath = args.strategy if args.strategy else settings.get('CRAWLING_STRATEGY')
    if not strategy_classpath:
        raise ValueError("Couldn't locate strategy class path. Please supply it either using command line option or "
                         "settings file.")
    strategy_class = load_object(strategy_classpath)

    partition_id = args.partition_id if args.partition_id is not None else settings.get('SCORING_PARTITION_ID')
    if partition_id >= settings.get('SPIDER_LOG_PARTITIONS') or partition_id < 0:
        raise ValueError("Partition id (%d) cannot be less than zero or more than SPIDER_LOG_PARTITIONS." %
                         partition_id)
    settings.set('SCORING_PARTITION_ID', partition_id)

    logging_config_path = settings.get("LOGGING_CONFIG")
    if logging_config_path and exists(logging_config_path):
        fileConfig(logging_config_path)
    else:
        logging.basicConfig(level=args.log_level)
        logger.setLevel(args.log_level)
        logger.addHandler(CONSOLE)
    return settings, strategy_class


if __name__ == '__main__':
    settings, strategy_class = setup_environment()
    worker = StrategyWorker(settings, strategy_class)
    worker.run()
