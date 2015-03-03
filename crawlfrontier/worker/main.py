# -*- coding: utf-8 -*-
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from scrapy.utils.serialize import ScrapyJSONDecoder, ScrapyJSONEncoder
from crawlfrontier.contrib.backends.remote import prepare_request_message
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

        self.decoder = ScrapyJSONDecoder()
        self.encoder = ScrapyJSONEncoder()
        self.manager = BaseManager.from_settings(self.settings)
        self.backend = self.manager.backend

        self.consumer_batch_size = self.settings.get('CONSUMER_BATCH_SIZE', 24)
        self.outgoing_topic = self.settings.get('OUTGOING_TOPIC')


    def _response_from_object(self, obj):
        request = self.manager._request_model(url=obj['url'],
                                              meta=obj['meta'])
        return self.manager._response_model(url=obj['url'],
                     status_code=obj['status_code'],
                     request=request)

    def _request_from_object(self, obj):
        return self.manager._request_model(url=obj['url'],
                                           method=obj['method'],
                                           headers=obj['headers'],
                                           cookies=obj['cookies'],
                                           meta=obj['meta'])

    def _request_to_object(self, request):
        return prepare_request_message(request)

    def start(self):
        self.new_batch()
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
                    if msg['type'] == 'add_seeds':
                        logger.info('Adding seeds')
                        seeds = []
                        for seed in msg['seeds']:
                            request = self._request_from_object(seed)
                            seeds.append(request)
                            logger.debug('URL: ', request.url)

                        self.backend.add_seeds(seeds)

                    if msg['type'] == 'page_crawled':
                        logger.info("Page crawled %s", msg['r']['url'])
                        response = self._response_from_object(msg['r'])
                        links = [self._request_from_object(link) for link in msg['links']]
                        self.backend.page_crawled(response, links)

                    if msg['type'] == 'request_error':
                        logger.info("Request error %s", msg['r']['url'])
                        request = self._request_from_object(msg['r'])
                        self.backend.request_error(request, msg['error'])
            self.new_batch()

    def new_batch(self):
        count = 0
        for request in self.backend.get_next_requests(self.settings.MAX_NEXT_REQUESTS):
            eo = self.encoder.encode(self._request_to_object(request))

            # TODO: send in batches
            self.producer.send_messages(self.outgoing_topic, request.meta['domain']['fingerprint'], eo)
            count += 1
        logger.info("Pushed new batch of %d items", count)


if __name__ == '__main__':
    worker = FrontierWorker(argv[1])
    worker.start()
