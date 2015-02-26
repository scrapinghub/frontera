# -*- coding: utf-8 -*-
from kafka import KafkaConsumer, SimpleConsumer
from scrapy.utils.serialize import ScrapyJSONDecoder
from crawlfrontier.core.manager import BaseManager
from crawlfrontier.settings import Settings

from sys import stderr, argv

class FrontierWorker(object):
    def __init__(self, settings):
        self.settings = Settings(module=settings)
        self.consumer = KafkaConsumer(settings.get('INCOMING_TOPIC'),
                                      group_id=settings.get('FRONTIER_GROUP'),
                                      metadata_broker_list=[settings.get('KAFKA_LOCATION')])
        self.is_finishing = False

        self.decoder = ScrapyJSONDecoder()
        self.manager = BaseManager.from_settings(settings)
        self.backend = self.manager.backend

    def start(self):
        while not self.is_finishing:
            for m in self.consumer:
                try:
                    obj = self.decoder.decode(m.value)
                except (KeyError, TypeError), e:
                    print >> stderr, "Decoding error: ", e
                    continue
                else:
                    response = self.manager._response_model(obj['response'])
                    links = [self.manager._request_model(link) for link in obj['links']]
                    self.backend.page_crawled(response, links)


if __name__ == '__main__':
    worker = FrontierWorker(argv[0])
    worker.start()
