# -*- coding: utf-8 -*-
from kafka import KafkaConsumer, SimpleConsumer
from scrapy.utils.serialize import ScrapyJSONDecoder
from crawlfrontier.core.manager import BaseManager
from crawlfrontier.settings import Settings

from sys import stderr, argv


class FrontierWorker(object):
    def __init__(self, module_name):
        self.settings = Settings(module=module_name)
        self.consumer = KafkaConsumer(self.settings.get('INCOMING_TOPIC'),
                                      group_id=self.settings.get('FRONTIER_GROUP'),
                                      metadata_broker_list=[self.settings.get('KAFKA_LOCATION')])
        self.is_finishing = False

        self.decoder = ScrapyJSONDecoder()
        self.manager = BaseManager.from_settings(self.settings)
        self.backend = self.manager.backend

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

    def start(self):
        while not self.is_finishing:
            for m in self.consumer:
                try:
                    msg = self.decoder.decode(m.value)
                except (KeyError, TypeError), e:
                    print >> stderr, "Decoding error: ", e
                    continue
                else:
                    if msg['type'] == 'add_seeds':
                        seeds = [self._request_from_object(seed) for seed in msg['seeds']]
                        self.backend.add_seeds(seeds)

                    if msg['type'] == 'page_crawled':
                        response = self._response_from_object(msg['r'])
                        links = [self.manager._request_model(link) for link in msg['links']]
                        self.backend.page_crawled(response, links)

                    if msg['type'] == 'request_error':
                        request = self._request_from_object(msg['r'])
                        self.backend.request_error(request, msg['error'])

if __name__ == '__main__':
    worker = FrontierWorker(argv[1])
    worker.start()
