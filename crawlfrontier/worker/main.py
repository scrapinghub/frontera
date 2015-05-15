# -*- coding: utf-8 -*-
import logging
from argparse import ArgumentParser
from time import asctime

from twisted.internet import reactor
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from kafka.common import OffsetOutOfRangeError

from crawlfrontier.contrib.backends.remote.codecs import KafkaJSONDecoder, KafkaJSONEncoder
from crawlfrontier.core.manager import FrontierManager
from crawlfrontier.settings import Settings
from crawlfrontier.worker.partitioner import Crc32NamePartitioner
from crawlfrontier.utils.url import parse_domain_from_url_fast
from utils import CallLaterOnce
from server import JsonRpcService
from offsets import Fetcher


logging.basicConfig()
logger = logging.getLogger("cf")


class Slot(object):
    def __init__(self, new_batch, consume_incoming, consume_scoring, no_batches, no_scoring, new_batch_delay):
        self.new_batch = CallLaterOnce(new_batch)
        self.new_batch.setErrback(self.error)

        self.consumption = CallLaterOnce(consume_incoming)
        self.consumption.setErrback(self.error)

        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)

        self.scoring_consumption = CallLaterOnce(consume_scoring)
        self.scoring_consumption.setErrback(self.error)

        self.is_finishing = False
        self.disable_new_batches = no_batches
        self.disable_scoring_consumption = no_scoring
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
            if not self.disable_scoring_consumption:
                self.scoring_consumption.schedule()
        self.scheduling.schedule(1.0)


class FrontierWorker(object):
    def __init__(self, settings, no_batches, no_scoring):
        self._kafka = KafkaClient(settings.get('KAFKA_LOCATION'))
        self._producer = KeyedProducer(self._kafka, partitioner=Crc32NamePartitioner)

        self._in_consumer = SimpleConsumer(self._kafka,
                                       settings.get('FRONTIER_GROUP'),
                                       settings.get('INCOMING_TOPIC'),
                                       buffer_size=1048576,
                                       max_buffer_size=10485760)
        if not no_scoring:
            self._scoring_consumer = SimpleConsumer(self._kafka,
                                           settings.get('FRONTIER_GROUP'),
                                           settings.get('SCORING_TOPIC'),
                                           buffer_size=262144,
                                           max_buffer_size=1048576)

        self._offset_fetcher = Fetcher(self._kafka, settings.get('OUTGOING_TOPIC'), settings.get('FRONTIER_GROUP'))

        self._manager = FrontierManager.from_settings(settings)
        self._backend = self._manager.backend
        self._encoder = KafkaJSONEncoder(self._manager.request_model)
        self._decoder = KafkaJSONDecoder(self._manager.request_model, self._manager.response_model)

        self.consumer_batch_size = settings.get('CONSUMER_BATCH_SIZE', 128)
        self.outgoing_topic = settings.get('OUTGOING_TOPIC')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.slot = Slot(self.new_batch, self.consume_incoming, self.consume_scoring, no_batches, no_scoring,
                         settings.get('NEW_BATCH_DELAY', 60.0))
        self.stats = {}

    def run(self):
        self.slot.schedule(on_start=True)
        reactor.run()

    def consume_incoming(self, *args, **kwargs):
        consumed = 0
        try:
            for m in self._in_consumer.get_messages(count=self.consumer_batch_size):
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
                        self._backend.add_seeds(seeds)

                    if type == 'page_crawled':
                        _, response, links = msg
                        logger.debug("Page crawled %s", response.url)

                        # FIXME: a dirty hack
                        filtered = []
                        for link in links:
                            if link.url.find('locanto') != -1:
                                continue

                            filtered.append(link)
                        self._backend.page_crawled(response, filtered)
                    if type == 'request_error':
                        _, request, error = msg
                        logger.info("Request error %s", request.url)
                        self._backend.request_error(request, error)
                finally:
                    consumed += 1
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self._in_consumer.seek(0, 2)  # moving to the tail of the log
            logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

        logger.info("Consumed %d items.", consumed)
        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        return consumed

    def consume_scoring(self, *args, **kwargs):
        consumed = 0
        try:
            batch = {}
            for m in self._scoring_consumer.get_messages(count=1024):
                try:
                    msg = self._decoder.decode(m.message.value)
                except (KeyError, TypeError), e:
                    logger.error("Decoding error: %s", e)
                    continue
                else:
                    _, fprint, score, url, schedule = msg
                    batch[fprint] = (score, url, schedule)
                finally:
                    consumed += 1
            self._backend.update_score(batch)
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self._scoring_consumer.seek(0, 2)  # moving to the tail of the log
            logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

        logger.info("Consumed %d items during scoring consumption.", consumed)
        self.stats['last_consumed_scoring'] = consumed
        self.stats['last_consumption_run_scoring'] = asctime()

    def new_batch(self, *args, **kwargs):
        lags = self._offset_fetcher.get()
        logger.info("Got lags %s" % str(lags))

        partitions = []
        for partition, lag in lags.iteritems():
            if lag < self.max_next_requests:
                partitions.append(partition)

        logger.info("Getting new batches for partitions %s" % str(",").join(map(str, partitions)))
        if not partitions:
            return 0

        count = 0
        for request in self._backend.get_next_requests(self.max_next_requests, partitions=partitions):
            try:
                eo = self._encoder.encode_request(request)
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
            self._producer.send_messages(self.outgoing_topic, encoded_name, eo)
        logger.info("Pushed new batch of %d items", count)
        self.stats['last_batch_size'] = count
        self.stats.setdefault('batches_after_start', 0)
        self.stats['batches_after_start'] += 1
        self.stats['last_batch_generated'] = asctime()
        return count

    def disable_new_batches(self):
        self.slot.disable_new_batches = True

    def enable_new_batches(self):
        self.slot.disable_new_batches = False

if __name__ == '__main__':
    parser = ArgumentParser(description="Crawl frontier worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables periodical generation of new batches')
    parser.add_argument('--no-scoring', action='store_true',
                        help='Disables periodical consumption of scoring topic')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen")
    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    if args.port:
        settings.set("JSONRPC_PORT", args.port)

    worker = FrontierWorker(settings, args.no_batches, args.no_scoring)
    server = JsonRpcService(worker, settings)
    server.start_listening()
    worker.run()

