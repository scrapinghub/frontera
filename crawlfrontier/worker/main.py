# -*- coding: utf-8 -*-
import logging
from argparse import ArgumentParser
from time import asctime

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from kafka.common import OffsetOutOfRangeError

from crawlfrontier.contrib.backends.remote.codecs import KafkaJSONDecoder, KafkaJSONEncoder
from crawlfrontier.core.manager import FrontierManager
from crawlfrontier.settings import Settings
from crawlfrontier.worker.partitioner import Crc32NamePartitioner
from crawlfrontier.utils.url import parse_domain_from_url_fast

from server import JsonRpcService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cf")


class CallLaterOnce(object):
    """Schedule a function to be called in the next reactor loop, but only if
    it hasn't been already scheduled since the last time it run.
    """
    def __init__(self, func, *a, **kw):
        self._func = func
        self._a = a
        self._kw = kw
        self._call = None
        self._errfunc = None
        self._erra = None
        self._errkw = None

    def setErrback(self, func, *a, **kw):
        self._errfunc = func
        self._erra = a
        self._errkw = kw

    def schedule(self, delay=0.0):
        if self._call is None:
            d = Deferred()
            d.addCallback(self)
            if self._errfunc:
                d.addErrback(self.error)
            self._call = reactor.callLater(delay, d.callback, None)

    def cancel(self):
        if self._call:
            self._call.cancel()

    def __call__(self, *args, **kwargs):
        self._call = None
        return self._func(*self._a, **self._kw)

    def error(self, f):
        self._call = None
        if self._errfunc:
            return self._errfunc(f, *self._erra, **self._errkw)
        return f


class Slot(object):
    def __init__(self, new_batch, consume, no_batches, new_batch_delay):
        self.new_batch = CallLaterOnce(new_batch)
        self.new_batch.setErrback(self.error)

        self.consumption = CallLaterOnce(consume)
        self.consumption.setErrback(self.error)

        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)

        self.is_finishing = False
        self.disable_new_batches = no_batches
        self.new_batch_delay = new_batch_delay

    def error(self, f):
        logger.error(f)
        reactor.stop()
        return f

    def schedule(self, on_start=False):
        if on_start and not self.disable_new_batches:
            self.new_batch.schedule(0)
        if not self.is_finishing:
            self.consumption.schedule()
            if not self.disable_new_batches:
                self.new_batch.schedule(self.new_batch_delay)
        self.scheduling.schedule(1.0)


class FrontierWorker(object):
    def __init__(self, settings, no_batches):
        self.kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self.producer = KeyedProducer(self.kafka, partitioner=Crc32NamePartitioner)

        self.consumer = SimpleConsumer(self.kafka,
                                       settings.get('FRONTIER_GROUP'),
                                       settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=10485760)

        self.manager = FrontierManager.from_settings(settings)
        self.backend = self.manager.backend
        self.encoder = KafkaJSONEncoder(self.manager.request_model)
        self.decoder = KafkaJSONDecoder(self.manager.request_model, self.manager.response_model)

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE', 128)
        self.outgoing_topic = settings.get('OUTGOING_TOPIC')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.slot = Slot(self.new_batch, self.consume, no_batches, settings.get('NEW_BATCH_DELAY', 60.0))
        self.stats = {}

    def run(self):
        self.slot.schedule(on_start=True)
        reactor.run()

    def consume(self, *args, **kwargs):
        consumed = 0
        try:
            for m in self.consumer.get_messages(count=self.consumer_batch_size):
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

                        # FIXME: a dirty hack
                        filtered = []
                        for link in links:
                            if link.url.find('locanto') != -1:
                                continue
                            filtered.append(link)
                        self.backend.page_crawled(response, filtered)

                    if type == 'request_error':
                        _, request, error = msg
                        logger.info("Request error %s", request.url)
                        self.backend.request_error(request, error)
                finally:
                    consumed += 1
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self.consumer.seek(0, 2)  # moving to the tail of the log
            logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

        logger.info("Consumed %d items.", consumed)
        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        return consumed

    def new_batch(self, *args, **kwargs):
        count = 0
        for request in self.backend.get_next_requests(self.max_next_requests):
            try:
                eo = self.encoder.encode_request(request)
            except Exception, e:
                logger.error("Encoding error, %s, fingerprint: %s, url: %s" % (e,
                                                                               request.meta['fingerprint'],
                                                                               request.url))
                continue
            finally:
                count +=1

            try:
                netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url_fast(request.url)
            except Exception, e:
                logger.error("URL parsing error %s, fingerprint %s, url %s" % (e, 
                                                                                request.meta['fingerprint'], 
                                                                                request.url))
            encoded_name = name.encode('utf-8', 'ignore')
            self.producer.send_messages(self.outgoing_topic, encoded_name, eo)
        logger.info("Pushed new batch of %d items", count)
        self.stats['last_batch_size'] = count
        self.stats.setdefault('batches_after_start', 0)
        self.stats['batches_after_start'] += 1
        return count


if __name__ == '__main__':
    parser = ArgumentParser(description="Crawl frontier worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables periodical generation of new batches')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    args = parser.parse_args()
    settings = Settings(module=args.config)
    worker = FrontierWorker(settings, args.no_batches)
    server = JsonRpcService(worker, settings)
    server.start_listening()
    worker.run()

