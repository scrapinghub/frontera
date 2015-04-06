# -*- coding: utf-8 -*-
from sys import argv
import logging
from time import time

from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from kafka.common import OffsetOutOfRangeError

from crawlfrontier.contrib.backends.remote.codecs import KafkaJSONDecoder, KafkaJSONEncoder
from crawlfrontier.core.manager import FrontierManager
from crawlfrontier.settings import Settings
from crawlfrontier.worker.partitioner import Crc32NamePartitioner
from crawlfrontier.utils.url import parse_domain_from_url_fast


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FrontierWorker(object):
    def __init__(self, module_name):
        self.settings = Settings(module=module_name)
        self.kafka = KafkaClient(self.settings.get('KAFKA_LOCATION'))
        self.producer = KeyedProducer(self.kafka, partitioner=Crc32NamePartitioner)

        self.consumer = SimpleConsumer(self.kafka,
                                       self.settings.get('FRONTIER_GROUP'),
                                       self.settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=10485760)
        self.is_finishing = False


        self.manager = FrontierManager.from_settings(self.settings)
        self.backend = self.manager.backend
        self.encoder = KafkaJSONEncoder(self.manager.request_model)
        self.decoder = KafkaJSONDecoder(self.manager.request_model, self.manager.response_model)


        self.consumer_batch_size = self.settings.get('CONSUMER_BATCH_SIZE', 128)
        self.outgoing_topic = self.settings.get('OUTGOING_TOPIC')

    def start(self):
        produced = self.new_batch()
        consumed = 0
        last_batch_timestamp = time()
        while not self.is_finishing:
            try:
                for m in self.consumer.get_messages(count=self.consumer_batch_size,
                                                    block=True,
                                                    timeout=30.0):
                    try:
                        msg = self.decoder.decode(m.message.value)
                    except (KeyError, TypeError), e:
                        logger.error("Decoding error: %s", e)
                        continue
                    else:
                        type = msg[0]
                        if type == 'add_seeds':
                            _, seeds = msg
                            logger.info('Adding %i seeds', len(seeds))
                            map(lambda seed: logger.debug('URL: ', seed.url), seeds)
                            self.backend.add_seeds(seeds)

                        if type == 'page_crawled':
                            _, response, links = msg
                            logger.debug("Page crawled %s", response.url)
                            self.backend.page_crawled(response, links)

                        if type == 'request_error':
                            _, request, error = msg
                            logger.info("Request error %s", request.url)
                            self.backend.request_error(request, error)
                    finally:
                        consumed += 1
            except OffsetOutOfRangeError, e:
                # https://github.com/mumrah/kafka-python/issues/263
                self.consumer.seek(0, 2)  # moving to the tail of the log
                continue

            logger.info("Consumed %d items.", consumed)
            now = time()
            if consumed > produced * 0.4 or now - last_batch_timestamp > 60.0:
                produced = self.new_batch()
                consumed = 0
                last_batch_timestamp = now

    def new_batch(self):
        count = 0
        for request in self.backend.get_next_requests(self.settings.MAX_NEXT_REQUESTS):
            try:
                eo = self.encoder.encode_request(request)
            except Exception, e:
                logger.error("Encoding error, %s, fingerprint: %s, url: %s" % (e,
                                                                               request.meta['fingerprint'],
                                                                               request.url))
                continue
            finally:
                count +=1

            netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url_fast(request.url)
            encoded_name = name.encode('utf-8', 'ignore')
            # TODO: send in batches
            self.producer.send_messages(self.outgoing_topic, encoded_name, eo)
        logger.info("Pushed new batch of %d items", count)
        return count


if __name__ == '__main__':
    worker = FrontierWorker(argv[1])
    worker.start()
