# -*- coding: utf-8 -*-
from crawlfrontier.settings import Settings
from crawlfrontier.contrib.backends.remote.codecs import KafkaJSONDecoder, KafkaJSONEncoder
from crawlfrontier.core.manager import FrontierManager
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
from kafka.common import OffsetOutOfRangeError

from time import asctime
from urlparse import urlparse
import logging
from argparse import ArgumentParser


logging.basicConfig()
logger = logging.getLogger("score")

class ScoringWorker(object):
    def __init__(self, settings):
        kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self._producer = SimpleProducer(kafka)

        self._in_consumer = SimpleConsumer(kafka,
                                       settings.get('SCORING_GROUP'),
                                       settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=10485760)

        self._manager = FrontierManager.from_settings(settings)
        self._decoder = KafkaJSONDecoder(self._manager.request_model, self._manager.response_model)
        self._encoder = KafkaJSONEncoder(self._manager.request_model)

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE', 128)
        self.outgoing_topic = settings.get('SCORING_TOPIC')
        self.domain_white_list = ["es.wikipedia.org", "www.dmoz.org"]
        self.stats = {}

    def get_score(self, url):
        url_parts = urlparse(url)

        if not url_parts.hostname.endswith(".es") and not url_parts.hostname in self.domain_white_list:
            return None

        path_parts = url_parts.path.split('/')
        return 1.0 / len(path_parts)

    def run(self):
        while True:
            consumed = 0
            try:
                for m in self._in_consumer.get_messages(count=self.consumer_batch_size, timeout=5.0):
                    batch = []
                    try:
                        msg = self._decoder.decode(m.message.value)
                    except (KeyError, TypeError), e:
                        logger.error("Decoding error: %s", e)
                        continue
                    else:
                        type = msg[0]
                        if type == 'add_seeds':
                            _, seeds = msg
                            logger.info('Adding %i seeds', len(seeds))
                            for seed in seeds:
                                logger.debug('URL: ', seed.url)
                                score = self.get_score(seed.url)
                                if score is not None:
                                    batch.append(self._encoder.encode_update_score(seed.meta['fingerprint'], score))


                        if type == 'page_crawled':
                            _, response, links = msg
                            logger.debug("Page crawled %s", response.url)
                            score = self.get_score(response.url)
                            if score is not None:
                                batch.append(self._encoder.encode_update_score(response.meta['fingerprint'], score))
                            for link in links:
                                score = self.get_score(link.url)
                                if score is not None:
                                    batch.append(self._encoder.encode_update_score(link.meta['fingerprint'], score))

                        if type == 'request_error':
                            # TODO: reduce score on specific types of errors
                            pass
                    finally:
                        consumed += 1
                        if batch:
                            self._producer.send_messages(self.outgoing_topic, *batch)

            except OffsetOutOfRangeError, e:
                # https://github.com/mumrah/kafka-python/issues/263
                self._in_consumer.seek(0, 2)  # moving to the tail of the log
                logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

            logger.info("Consumed %d items.", consumed)
            self.stats['last_consumed'] = consumed
            self.stats['last_consumption_run'] = asctime()

if __name__ == '__main__':
    parser = ArgumentParser(description="Crawl frontier scoring worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    worker = ScoringWorker(settings)
    worker.run()