# -*- coding: utf-8 -*-
from time import asctime
import logging
from argparse import ArgumentParser
from frontera.utils.misc import load_object

from frontera.core.manager import FrontierManager
from frontera.logger.handlers import CONSOLE
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from frontera.settings import Settings
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder


logger = logging.getLogger("strategy-worker")


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

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE')
        self.strategy = strategy_class.from_worker(settings)
        self.states = self._manager.backend.states
        self.stats = {}
        self.cache_flush_counter = 0
        self.job_id = 0
        self.task = LoopingCall(self.work)

    def work(self):
        consumed = 0
        batch = []
        fingerprints = set()
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
                    fingerprints.update(map(lambda x: x.meta['fingerprint'], seeds))
                    continue

                if type == 'page_crawled':
                    _, response, links = msg
                    fingerprints.add(response.meta['fingerprint'])
                    fingerprints.update(map(lambda x: x.meta['fingerprint'], links))
                    continue

                if type == 'request_error':
                    _, request, error = msg
                    fingerprints.add(request.meta['fingerprint'])
                    continue

                if type == 'offset':
                    continue
                raise TypeError('Unknown message type %s' % type)
            finally:
                consumed += 1

        self.states.fetch(fingerprints)
        fingerprints.clear()
        results = []
        for msg in batch:
            if len(results) > 1024:
                self.scoring_log_producer.send(None, *results)
                results = []

            type = msg[0]
            if type == 'add_seeds':
                _, seeds = msg
                for seed in seeds:
                    seed.meta['jid'] = self.job_id
                results.extend(self.on_add_seeds(seeds))
                continue

            if type == 'page_crawled':
                _, response, links = msg
                if response.meta['jid'] != self.job_id:
                    continue
                results.extend(self.on_page_crawled(response, links))
                continue

            if type == 'request_error':
                _, request, error = msg
                if request.meta['jid'] != self.job_id:
                    continue
                results.extend(self.on_request_error(request, error))
                continue
        if len(results):
            self.scoring_log_producer.send(None, *results)

        if self.cache_flush_counter == 30:
            logger.info("Flushing states")
            self.states.flush(force_clear=False)
            logger.info("Flushing states finished")
            self.cache_flush_counter = 0

        self.cache_flush_counter += 1

        if self.strategy.finished():
            logger.info("Successfully reached the crawling goal. Exiting.")
            exit(0)

        logger.info("Consumed %d items.", consumed)
        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()

    def run(self):
        self.task.start(interval=0)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        reactor.run()

    def stop(self):
        logger.info("Closing crawling strategy.")
        self.strategy.close()
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def on_add_seeds(self, seeds):
        logger.info('Adding %i seeds', len(seeds))
        seed_map = dict(map(lambda seed: (seed.meta['fingerprint'], seed), seeds))
        self.states.set_states(seeds)
        scores = self.strategy.add_seeds(seeds)
        self.states.update_cache(seeds)

        output = []
        for fingerprint, score in scores.iteritems():
            seed = seed_map[fingerprint]
            logger.debug('URL: %s', seed.url)
            if score is not None:
                encoded = self._encoder.encode_update_score(
                    seed.meta['fingerprint'],
                    score,
                    seed.url,
                    True
                )
                output.append(encoded)
        return output

    def on_page_crawled(self, response, links):
        logger.debug("Page crawled %s", response.url)
        objs_list = [response]
        objs_list.extend(links)
        objs = dict(map(lambda obj: (obj.meta['fingerprint'], obj), objs_list))
        self.states.set_states(objs_list)
        scores = self.strategy.page_crawled(response, links)
        self.states.update_cache(objs_list)

        output = []
        for fingerprint, score in scores.iteritems():
            obj = objs[fingerprint]
            if score is not None:
                encoded = self._encoder.encode_update_score(
                    obj.meta['fingerprint'],
                    score,
                    obj.url,
                    True
                )
                output.append(encoded)
        return output

    def on_request_error(self, request, error):
        self.states.set_states(request)
        scores = self.strategy.page_error(request, error)
        self.states.update_cache(request)
        assert len(scores) == 1
        fingerprint, score = scores.popitem()
        if score is not None:
            encoded = self._encoder.encode_update_score(
                request.meta['fingerprint'],
                score,
                request.url,
                False
            )
            return [encoded]
        return []


if __name__ == '__main__':
    parser = ArgumentParser(description="Frontera strategy worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--strategy', type=str,
                        help='Crawling strategy class path')

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    logger.addHandler(CONSOLE)
    settings = Settings(module=args.config)
    strategy_classpath = args.strategy if args.strategy else settings.get('CRAWLING_STRATEGY')
    if not strategy_classpath:
        raise ValueError("Couldn't locate strategy class path. Please supply it either using command line option or "
                         "settings file.")
    strategy_class = load_object(strategy_classpath)
    worker = StrategyWorker(settings, strategy_class)
    worker.run()
