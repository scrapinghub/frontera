# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera import Backend
from frontera.core import OverusedBuffer
from frontera.utils.misc import load_object
import logging
import six


class MessageBusBackend(Backend):
    def __init__(self, manager):
        settings = manager.settings
        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.mb = messagebus(settings)
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        store_content = settings.get('STORE_CONTENT')
        self._encoder = encoder_cls(manager.request_model, send_body=store_content)
        self._decoder = decoder_cls(manager.request_model, manager.response_model)
        self.spider_log_producer = self.mb.spider_log().producer()
        spider_feed = self.mb.spider_feed()
        self.partition_id = int(settings.get('SPIDER_PARTITION_ID'))
        if self.partition_id < 0 or self.partition_id >= settings.get('SPIDER_FEED_PARTITIONS'):
            raise ValueError("Spider partition id cannot be less than 0 or more than SPIDER_FEED_PARTITIONS.")
        self.consumer = spider_feed.consumer(partition_id=self.partition_id)
        self._get_timeout = float(settings.get('KAFKA_GET_TIMEOUT'))
        self._logger = logging.getLogger("messagebus-backend")
        self._buffer = OverusedBuffer(self._get_next_requests,
                                      max_per_key=settings.get('OVERUSED_MAX_PER_KEY'),
                                      keep_per_key=settings.get("OVERUSED_KEEP_PER_KEY"),
                                      max_keys=settings.get('OVERUSED_MAX_KEYS'),
                                      keep_keys=settings.get('OVERUSED_KEEP_KEYS'))
        self._logger.info("Consuming from partition id %d", self.partition_id)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.spider_log_producer.flush()
        self.consumer.close()

    def add_seeds(self, seeds):
        raise NotImplemented("The seeds addition using spider log isn't allowed")

    def page_crawled(self, response):
        host_fprint = get_host_fprint(response)
        self.spider_log_producer.send(host_fprint, self._encoder.encode_page_crawled(response))

    def links_extracted(self, request, links):
        per_host = aggregate_per_host(links)
        for host_fprint, host_links in six.iteritems(per_host):
            self.spider_log_producer.send(host_fprint,
                                          self._encoder.encode_links_extracted(request, host_links))

    def request_error(self, page, error):
        host_fprint = get_host_fprint(page)
        self.spider_log_producer.send(host_fprint, self._encoder.encode_request_error(page, error))

    def _get_next_requests(self, max_n_requests, **kwargs):
        requests = []
        for encoded in self.consumer.get_messages(count=max_n_requests, timeout=self._get_timeout):
            try:
                request = self._decoder.decode_request(encoded)
            except Exception as exc:
                self._logger.warning("Could not decode message: {0}, error {1}".format(encoded, str(exc)))
            else:
                requests.append(request)
        self.spider_log_producer.send(b'0123456789abcdef0123456789abcdef012345678',
                                      self._encoder.encode_offset(self.partition_id,
                                                                  self.consumer.get_offset(self.partition_id)))
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


def aggregate_per_host(requests):
    per_host = dict()
    for link in requests:
        if b'fingerprint' not in link.meta[b'domain']:
            continue
        host_fprint = link.meta[b'domain'][b'fingerprint']
        if host_fprint not in per_host:
            per_host[host_fprint] = []
        per_host[host_fprint].append(link)
    return per_host


def get_host_fprint(request):
    if b'fingerprint' not in request.meta[b'domain']:
        return None
    return request.meta[b'domain'][b'fingerprint']