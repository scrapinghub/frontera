# -*- coding: utf-8 -*-
from crawlfrontier.settings import Settings
from crawlfrontier.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
from crawlfrontier.core.manager import FrontierManager
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
from kafka.common import OffsetOutOfRangeError
from kafka.protocol import CODEC_SNAPPY

from time import asctime
import logging
from argparse import ArgumentParser
from importlib import import_module


logging.basicConfig()
logger = logging.getLogger("score")


class ScoringWorker(object):
    def __init__(self, settings, strategy_module):
        kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self._producer = SimpleProducer(kafka, codec=CODEC_SNAPPY)
        partition_id = settings.get('SCORING_PARTITION_ID')
        if partition_id == None or type(partition_id) != int:
            raise AttributeError("Scoring worker partition id isn't set.")
        self._in_consumer = SimpleConsumer(kafka,
                                       settings.get('SCORING_GROUP'),
                                       settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=10485760,
                                       partitions=[partition_id])

        self._manager = FrontierManager.from_settings(settings)
        self._decoder = Decoder(self._manager.request_model, self._manager.response_model)
        self._encoder = Encoder(self._manager.request_model)

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE', 128)
        self.outgoing_topic = settings.get('SCORING_TOPIC')
        self.strategy = strategy_module.CrawlStrategy()
        self.backend = self._manager.backend
        self.stats = {}

    def run(self):
        cache_flush_counter = 0
        while True:
            consumed = 0
            batch = []
            fingerprints = set()
            try:
                for m in self._in_consumer.get_messages(count=self.consumer_batch_size, block=True, timeout=1.0):
                    try:
                        msg = self._decoder.decode(m.message.value)
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

                        raise TypeError('Unknown message type %s' % type)
                    finally:
                        consumed += 1
            except OffsetOutOfRangeError, e:
                # https://github.com/mumrah/kafka-python/issues/263
                self._in_consumer.seek(0, 2)  # moving to the tail of the log
                logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

            self.backend.fetch_states(list(fingerprints))
            fingerprints.clear()
            results = []
            for msg in batch:
                if len(results) > 1024:
                    self._producer.send_messages(self.outgoing_topic, *results)
                    results = []

                type = msg[0]
                if type == 'add_seeds':
                    _, seeds = msg
                    results.extend(self.on_add_seeds(seeds))
                    continue

                if type == 'page_crawled':
                    _, response, links = msg
                    results.extend(self.on_page_crawled(response, links))
                    continue

                if type == 'request_error':
                    _, request, error = msg
                    results.extend(self.on_request_error(request, error))
                    continue
            if len(results):
                self._producer.send_messages(self.outgoing_topic, *results)

            if cache_flush_counter == 30:
                logger.info("Flushing states")
                self.backend.flush_states(is_clear=False)
                logger.info("Flushing states finished")
                cache_flush_counter = 0

            cache_flush_counter += 1

            if self.strategy.finished():
                logger.info("Succesfully reached the crawling goal. Exiting.")
                exit(0)

            logger.info("Consumed %d items.", consumed)
            self.stats['last_consumed'] = consumed
            self.stats['last_consumption_run'] = asctime()

    def on_add_seeds(self, seeds):
        logger.info('Adding %i seeds', len(seeds))
        seed_map = dict(map(lambda seed: (seed.meta['fingerprint'], seed), seeds))
        self.backend.update_states(seeds, False)
        scores = self.strategy.add_seeds(seeds)
        self.backend.update_states(seeds, True)

        output = []
        for fingerprint, score in scores.iteritems():
            seed = seed_map[fingerprint]
            logger.debug('URL: ', seed.url)
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
        self.backend.update_states(objs_list, False)
        scores = self.strategy.page_crawled(response, links)
        self.backend.update_states(objs_list, True)

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
        self.backend.update_states(request, False)
        scores = self.strategy.page_error(request, error)
        self.backend.update_states(request, True)
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
    parser = ArgumentParser(description="Crawl frontier scoring worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--strategy', type=str, required=True,
                        help='Crawling strategy module name')

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    strategy_module = import_module(args.strategy)
    worker = ScoringWorker(settings, strategy_module)
    worker.run()
