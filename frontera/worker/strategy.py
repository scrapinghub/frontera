# -*- coding: utf-8 -*-
from time import asctime
import logging
from logging.config import fileConfig
from argparse import ArgumentParser
from os.path import exists
from frontera.utils.misc import load_object

from frontera.core.manager import FrontierManager
from frontera.logger.handlers import CONSOLE
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from frontera.settings import Settings
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
from collections import Sequence


logger = logging.getLogger("strategy-worker")


class UpdateScoreStream(object):
    def __init__(self, encoder, scoring_log_producer, size):
        self._encoder = encoder
        self._buffer = []
        self._producer = scoring_log_producer
        self._size = size

    def send(self, url, fingerprint, score=1.0, dont_queue=False):
        encoded = self._encoder.encode_update_score(
            fingerprint,
            score,
            url,
            not dont_queue
        )
        self._buffer.append(encoded)
        if len(self._buffer) > self._size:
            self.flush()

    def flush(self):
        self._producer.send(None, *self._buffer)
        self._buffer = []


class StatesContext(object):

    def __init__(self, states):
        self._requests = []
        self._states = states
        self._fingerprints = set()
        self.cache_flush_counter = 0

    def to_fetch(self, requests):
        if isinstance(requests, Sequence):
            self._fingerprints.update(map(lambda x: x.meta['fingerprint'], requests))
            return
        self._fingerprints.add(requests.meta['fingerprint'])

    def fetch(self):
        self._states.fetch(self._fingerprints)
        self._fingerprints.clear()

    def refresh_and_keep(self, request):
        self._states.fetch([request.meta['fingerprint']])
        self._states.set_states(request)
        self._requests.append(request)

    def release(self):
        self._states.update_cache(self._requests)
        self._requests = []

        # Flushing states cache if needed
        if self.cache_flush_counter == 30:
            logger.info("Flushing states")
            self._states.flush(force_clear=False)
            logger.info("Flushing states finished")
            self.cache_flush_counter = 0

        self.cache_flush_counter += 1


class StrategyWorker(object):
    def __init__(self, settings, strategy_class):
        partition_id = settings.get('SCORING_PARTITION_ID')
        if partition_id is None or type(partition_id) != int:
            raise AttributeError("Scoring worker partition id isn't set.")

        messagebus = load_object(settings.get('MESSAGE_BUS'))
        mb = messagebus(settings)
        spider_log = mb.spider_log()
        scoring_log = mb.scoring_log()
        self.consumer = spider_log.consumer(partition_id=partition_id, type='sw')
        self.scoring_log_producer = scoring_log.producer()

        self._manager = FrontierManager.from_settings(settings, strategy_worker=True)
        self._decoder = Decoder(self._manager.request_model, self._manager.response_model)
        self._encoder = Encoder(self._manager.request_model)

        self.update_score = UpdateScoreStream(self._encoder, self.scoring_log_producer, 1024)
        self.states_context = StatesContext(self._manager.backend.states)

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE')
        self.strategy = strategy_class.from_worker(self._manager, self.update_score, self.states_context)
        self.states = self._manager.backend.states
        self.stats = {
            'consumed_since_start': 0
        }
        self.job_id = 0
        self.task = LoopingCall(self.work)
        self._logging_task = LoopingCall(self.log_status)
        logger.info("Strategy worker is initialized and consuming partition %d", partition_id)

    def work(self):
        # Collecting batch to process
        consumed = 0
        batch = []
        for m in self.consumer.get_messages(count=self.consumer_batch_size, timeout=1.0):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError), e:
                logger.error("Decoding error: %s", e)
                continue
            else:
                type = msg[0]
                batch.append(msg)
                if type == 'add_seeds':
                    _, seeds = msg
                    self.states_context.to_fetch(seeds)
                    continue

                if type == 'page_crawled':
                    _, response, links = msg
                    self.states_context.to_fetch(response)
                    self.states_context.to_fetch(links)
                    continue

                if type == 'request_error':
                    _, request, error = msg
                    self.states_context.to_fetch(request)
                    continue

                if type == 'offset':
                    continue
                raise TypeError('Unknown message type %s' % type)
            finally:
                consumed += 1

        # Fetching states
        self.states_context.fetch()

        # Batch processing
        for msg in batch:
            type = msg[0]
            if type == 'add_seeds':
                _, seeds = msg
                for seed in seeds:
                    seed.meta['jid'] = self.job_id
                self.on_add_seeds(seeds)
                continue

            if type == 'page_crawled':
                _, response, links = msg
                if response.meta['jid'] != self.job_id:
                    continue
                self.on_page_crawled(response, links)
                continue

            if type == 'request_error':
                _, request, error = msg
                if request.meta['jid'] != self.job_id:
                    continue
                self.on_request_error(request, error)
                continue

        self.update_score.flush()
        self.states_context.release()

        # Exiting, if crawl is finished
        if self.strategy.finished():
            logger.info("Successfully reached the crawling goal.")
            logger.info("Closing crawling strategy.")
            self.strategy.close()
            logger.info("Exiting.")
            exit(0)

        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        self.stats['consumed_since_start'] += consumed

    def run(self):
        self.task.start(interval=0)
        self._logging_task.start(interval=30)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        reactor.run()

    def log_status(self):
        for k, v in self.stats.iteritems():
            logger.info("%s=%s", k, v)

    def stop(self):
        logger.info("Closing crawling strategy.")
        self.strategy.close()
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def on_add_seeds(self, seeds):
        logger.info('Adding %i seeds', len(seeds))
        self.states.set_states(seeds)
        self.strategy.add_seeds(seeds)
        self.states.update_cache(seeds)

    def on_page_crawled(self, response, links):
        logger.debug("Page crawled %s", response.url)
        objs_list = [response]
        objs_list.extend(links)
        self.states.set_states(objs_list)
        self.strategy.page_crawled(response, links)
        self.states.update_cache(links)
        self.states.update_cache(response)

    def on_request_error(self, request, error):
        logger.debug("Page error %s (%s)", request.url, error)
        self.states.set_states(request)
        self.strategy.page_error(request, error)
        self.states.update_cache(request)


if __name__ == '__main__':
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
    worker = StrategyWorker(settings, strategy_class)
    worker.run()
