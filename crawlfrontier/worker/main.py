# -*- coding: utf-8 -*-
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from crawlfrontier.contrib.backends.remote.codecs import KafkaJSONDecoder, KafkaJSONEncoder
from crawlfrontier.core.manager import BaseManager
from crawlfrontier.settings import Settings

from kafka.partitioner.base import Partitioner

from struct import unpack
from sys import argv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions):
        size = len(partitions)
        value = unpack("<I", key[:4])
        idx = value[0] % size
        return partitions[idx]


class FrontierWorker(object):
    def __init__(self, module_name):
        self.settings = Settings(module=module_name)
        self.kafka = KafkaClient(self.settings.get('KAFKA_LOCATION'))
        self.producer = KeyedProducer(self.kafka, partitioner=FingerprintPartitioner)

        self.consumer = SimpleConsumer(self.kafka,
                                       self.settings.get('FRONTIER_GROUP'),
                                       self.settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=1048576)
        self.is_finishing = False


        self.manager = BaseManager.from_settings(self.settings)
        self.backend = self.manager.backend
        self.encoder = KafkaJSONEncoder(self.manager.request_model)
        self.decoder = KafkaJSONDecoder(self.manager.request_model, self.manager.response_model)


        self.consumer_batch_size = self.settings.get('CONSUMER_BATCH_SIZE', 24)
        self.outgoing_topic = self.settings.get('OUTGOING_TOPIC')

    def start(self):
        produced = self.new_batch()

        if not produced:
            logger.info("No requests were produced. Exiting.")
            return

        consumed = 0
        while not self.is_finishing:
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
                        logger.info("Page crawled %s", response.url)
                        self.backend.page_crawled(response, links)

                    if type == 'request_error':
                        _, request, error = msg
                        logger.info("Request error %s", request.url)
                        self.backend.request_error(request, error)
                finally:
                    consumed += 1

            logger.info("Consumed %d items.", consumed)
            if consumed > produced * 0.7:
                produced = self.new_batch()
                consumed = 0

    def new_batch(self):
        count = 0
        for request in self.backend.get_next_requests(self.settings.MAX_NEXT_REQUESTS):
            eo = self.encoder.encode_request(request)
            # TODO: send in batches
            self.producer.send_messages(self.outgoing_topic, request.meta['domain']['fingerprint'], eo)
            count += 1
        logger.info("Pushed new batch of %d items", count)
        return count


if __name__ == '__main__':
    worker = FrontierWorker(argv[1])
    worker.start()
