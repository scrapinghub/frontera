# -*- coding: utf-8 -*-
from frontera import Backend
from frontera.core import OverusedBuffer
from codecs.msgpack import Encoder, Decoder
from frontera.utils.misc import load_object
import logging


class MessageBusBackend(Backend):
    def __init__(self, manager):
        settings = manager.settings
        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.mb = messagebus(settings)
        store_content = settings.get('STORE_CONTENT')
        self._encoder = Encoder(manager.request_model, send_body=store_content)
        self._decoder = Decoder(manager.request_model, manager.response_model)
        self.spider_log_producer = self.mb.spider_log().producer()
        spider_feed = self.mb.spider_feed()
        self.partition_id = int(settings.get('SPIDER_PARTITION_ID'))
        if self.partition_id < 0 or self.partition_id >= settings.get('SPIDER_FEED_PARTITIONS'):
            raise ValueError("Spider partition id cannot be less than 0 or more than SPIDER_FEED_PARTITIONS.")
        self.consumer = spider_feed.consumer(partition_id=self.partition_id)
        self._get_timeout = float(settings.get('KAFKA_GET_TIMEOUT'))
        self._logger = logging.getLogger("messagebus-backend")
        self._buffer = OverusedBuffer(self._get_next_requests,
                                      self._logger.debug)
        self._logger.info("Consuming from partition id %d", self.partition_id)

    @classmethod
    def from_manager(clas, manager):
        return clas(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.spider_log_producer.flush()

    def add_seeds(self, seeds):
        self.spider_log_producer.send(seeds[0].meta['fingerprint'], self._encoder.encode_add_seeds(seeds))

    def page_crawled(self, response, links):
        self.spider_log_producer.send(response.meta['fingerprint'], self._encoder.encode_page_crawled(response, links))

    def request_error(self, page, error):
        self.spider_log_producer.send(page.meta['fingerprint'], self._encoder.encode_request_error(page, error))

    def _get_next_requests(self, max_n_requests, **kwargs):
        requests = []
        for encoded in self.consumer.get_messages(count=max_n_requests, timeout=self._get_timeout):
            try:
                request = self._decoder.decode_request(encoded)
            except Exception, exc:
                self._logger.warning("Could not decode message: {0}, error {1}".format(encoded, str(exc)))
            else:
                requests.append(request)
        self.spider_log_producer.send('0123456789abcdef0123456789abcdef012345678',
                                      self._encoder.encode_offset(self.partition_id, self.consumer.get_offset()))
        return requests

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._buffer.get_next_requests(max_n_requests, **kwargs)

    def finished(self):
        return False

    @property
    def metadata(self):
        return None

    @property
    def queue(self):
        return None

    @property
    def states(self):
        return None