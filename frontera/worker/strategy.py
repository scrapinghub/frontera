# -*- coding: utf-8 -*-
from __future__ import absolute_import
from time import asctime
import logging
from traceback import format_stack, format_tb
from signal import signal, SIGUSR1
from logging.config import fileConfig
from argparse import ArgumentParser
from os.path import exists
from random import randint
from frontera.utils.misc import load_object
from frontera.utils.ossignal import install_shutdown_handlers

from frontera.core.manager import FrontierManager
from frontera.logger.handlers import CONSOLE
from frontera.worker.stats import StatsExportMixin
from frontera.worker.server import WorkerJsonRpcService

from twisted.internet.task import LoopingCall
from twisted.internet import reactor, task
from twisted.internet.defer import Deferred

from frontera.settings import Settings
from collections import Iterable
from binascii import hexlify
import six
from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen


logger = logging.getLogger("strategy-worker")


class UpdateScoreStream(object):

    def __init__(self, producer, encoder):
        self._producer = producer
        self._encoder = encoder

    def send(self, request, score=1.0, dont_queue=False):
        encoded = self._encoder.encode_update_score(
            request=request,
            score=score,
            schedule=not dont_queue
        )
        self._producer.send(None, encoded)

    def flush(self):
        pass


class StatesContext(object):

    def __init__(self, states, debug_mode=False, debug_stream=False):
        self._requests = []
        self._states = states
        self._fingerprints = dict()
        self._debug_stream = debug_stream
        self.debug_mode = debug_mode

    def to_fetch(self, requests):
        requests = requests if isinstance(requests, Iterable) else [requests]
        for request in requests:
            fingerprint = request.meta[b'fingerprint']
            self._fingerprints[fingerprint] = request if self.debug_mode else None

    def fetch(self):
        self._states.fetch(self._fingerprints, debug_stream=self._debug_stream)
        self._fingerprints.clear()

    def refresh_and_keep(self, requests):
        self.to_fetch(requests)
        self.fetch()
        self._states.set_states(requests)
        self._requests.extend(requests)

    def release(self):
        self._states.update_cache(self._requests)
        self._requests = []

    def flush(self):
        logger.info("Flushing states")
        self._states.flush()
        logger.info("Flushing of states finished")


class BaseStrategyWorker(object):
    """Base strategy worker class."""

    def __init__(self, settings, strategy_class, strategy_args, is_add_seeds_mode):
        partition_id = settings.get('SCORING_PARTITION_ID')
        if partition_id is None or type(partition_id) != int:
            raise AttributeError("Scoring worker partition id isn't set.")

        messagebus = load_object(settings.get('MESSAGE_BUS'))
        mb = messagebus(settings)
        scoring_log = mb.scoring_log()
        self.add_seeds_mode = is_add_seeds_mode
        if not self.add_seeds_mode:
            spider_log = mb.spider_log()
            self.consumer = spider_log.consumer(partition_id=partition_id, type=b'sw')
            self.consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')
        self.scoring_log_producer = scoring_log.producer()

        self._manager = FrontierManager.from_settings(settings, strategy_worker=True)
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        self._decoder = decoder_cls(self._manager.request_model, self._manager.response_model)
        self._encoder = encoder_cls(self._manager.request_model)

        self.update_score = UpdateScoreStream(self.scoring_log_producer, self._encoder)
        self.states_context = StatesContext(self._manager.backend.states)
        self.consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')
        self.strategy = strategy_class.from_worker(self._manager, self.update_score, self.states_context)
        self.states = self._manager.backend.states
        self.stats = {
            'consumed_since_start': 0,
            'consumed_add_seeds': 0,
            'consumed_page_crawled': 0,
            'consumed_links_extracted': 0,
            'consumed_request_error': 0,
            'dropped_links_extracted': 0,
        }
        self.job_id = 0
        self.task = LoopingCall(self.work)
        self._logging_task = LoopingCall(self.log_status)
        self._flush_states_task = LoopingCall(self.flush_states)
        self._flush_interval = settings.get("SW_FLUSH_INTERVAL")
        logger.info("Strategy worker is initialized and consuming partition %d", partition_id)

    def collect_unknown_message(self, msg):
        logger.debug('Unknown message %s', msg)

    def on_unknown_message(self, msg):
        pass

    def collect_batch(self):
        consumed = 0
        batch = []
        for m in self.consumer.get_messages(count=self.consumer_batch_size, timeout=1.0):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error:")
                logger.exception(e)
                logger.debug("Message %s", hexlify(m))
                continue
            else:
                type = msg[0]
                batch.append(msg)
                try:
                    if type == 'page_crawled':
                        _, response = msg
                        self.states_context.to_fetch(response)
                        continue
                    if type == 'links_extracted':
                        _, request, links = msg
                        self.states_context.to_fetch(request)
                        filtered_links = self.strategy.filter_extracted_links(request, links)
                        if filtered_links:
                            # modify last message with a new links list
                            batch[-1] = (type, request, filtered_links)
                            self.states_context.to_fetch(filtered_links)
                        else:
                            # drop last message if nothing to process
                            batch.pop()
                            self.stats['dropped_links_extracted'] += 1
                        continue
                    if type == 'request_error':
                        _, request, error = msg
                        self.states_context.to_fetch(request)
                        continue
                    if type == 'offset':
                        continue
                    self.collect_unknown_message(msg)
                except Exception as exc:
                    logger.exception(exc)
                    pass
            finally:
                consumed += 1
        return (batch, consumed)

    def process_batch(self, batch):
        for msg in batch:
            type = msg[0]
            try:
                if type == 'page_crawled':
                    _, response = msg
                    if b'jid' not in response.meta or response.meta[b'jid'] != self.job_id:
                        continue
                    self.on_page_crawled(response)
                    self.stats['consumed_page_crawled'] += 1
                    continue
                if type == 'links_extracted':
                    _, request, links = msg
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self.on_links_extracted(request, links)
                    self.stats['consumed_links_extracted'] += 1
                    continue
                if type == 'request_error':
                    _, request, error = msg
                    if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                        continue
                    self.on_request_error(request, error)
                    self.stats['consumed_request_error'] += 1
                    continue
                self.on_unknown_message(msg)
            except Exception as exc:
                logger.exception(exc)
                pass

    def work(self):
        batch, consumed = self.collect_batch()
        self.states_context.fetch()
        self.process_batch(batch)
        self.update_score.flush()
        self.states_context.release()

        # Exiting, if crawl is finished
        if self.strategy.finished():
            logger.info("Successfully reached the crawling goal.")
            logger.info("Finishing.")
            d = self.stop_tasks()
            reactor.callLater(0, d.callback, None)

        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        self.stats['consumed_since_start'] += consumed

    def add_seeds(self, seeds_url):
        logger.info("Seeds addition started from url %s", seeds_url)
        if not seeds_url:
            self.strategy.read_seeds(None)
        else:
            parsed = urlparse(seeds_url)
            if parsed.scheme == "s3":
                import boto3
                from frontera.utils.s3 import StreamingBodyIOBase
                s3 = boto3.resource("s3")
                path = parsed.path.lstrip("/")
                obj = s3.Object(parsed.hostname, path)
                response = obj.get()
                fh = StreamingBodyIOBase(response['Body'])
            elif parsed.scheme == "file":
                fh = open(parsed.path, "rb")
            else:
                raise TypeError("Unsupported URL scheme")
            self.strategy.read_seeds(fh)
            try:
                fh.close()
            except:
                logger.exception("Error during closing of seeds stream")
                pass
        self.update_score.flush()
        self.states_context.release()

    def run(self, seeds_url):
        def log_failure(failure):
            logger.exception(failure.value)
            if failure.frames:
                logger.critical(str("").join(format_tb(failure.getTracebackObject())))

        def errback_main(failure):
            log_failure(failure)
            self.task.start(interval=0).addErrback(errback_main)

        def run_flush_states_task():
            (self._flush_states_task.start(interval=self._flush_interval)
                                    .addErrback(errback_flush_states))

        def errback_flush_states(failure):
            log_failure(failure)
            run_flush_states_task()

        def debug(sig, frame):
            logger.critical("Signal received: printing stack trace")
            logger.critical(str("").join(format_stack(frame)))

        install_shutdown_handlers(self._handle_shutdown)
        signal(SIGUSR1, debug)
        if self.add_seeds_mode:
            self.add_seeds(seeds_url)
            d = self.stop_tasks()
            reactor.callLater(0, d.callback, None)
        else:
            self.task.start(interval=0).addErrback(errback_main)
            self._logging_task.start(interval=30)
            # run flushing states LoopingCall with random delay
            flush_states_task_delay = randint(0, self._flush_interval)
            logger.info("Starting flush-states task in %d seconds", flush_states_task_delay)
            task.deferLater(reactor, flush_states_task_delay, run_flush_states_task)

        reactor.run(installSignalHandlers=False)

    def log_status(self):
        for k, v in six.iteritems(self.stats):
            logger.info("%s=%s", k, v)

    def flush_states(self):
        self.states_context.flush()

    def _handle_shutdown(self, signum, _):
        def call_shutdown():
            d = self.stop_tasks()
            reactor.callLater(0, d.callback, None)

        logger.info("Received shutdown signal %d, shutting down gracefully.", signum)
        reactor.callFromThread(call_shutdown)

    def stop_tasks(self):
        logger.info("Stopping periodic tasks.")
        if self.task.running:
            self.task.stop()
        if self._flush_states_task.running:
            self._flush_states_task.stop()
        if self._logging_task.running:
            self._logging_task.stop()

        d = Deferred()
        d.addBoth(self._perform_shutdown)
        d.addBoth(self._stop_reactor)
        return d

    def _stop_reactor(self, _=None):
        logger.info("Stopping reactor.")
        try:
            reactor.stop()
        except RuntimeError:  # raised if already stopped or in shutdown stage
            pass

    def _perform_shutdown(self, _=None):
        try:
            self.flush_states()
            logger.info("Closing crawling strategy.")
            self.strategy.close()
            logger.info("Stopping frontier manager.")
            self._manager.stop()
            logger.info("Closing message bus.")
            self.scoring_log_producer.close()
            if not self.add_seeds_mode:
                self.consumer.close()
        except:
            logger.exception('Error on shutdown')

    def on_page_crawled(self, response):
        logger.debug("Page crawled %s", response.url)
        self.states.set_states([response])
        self.strategy.page_crawled(response)
        self.states.update_cache(response)

    def on_links_extracted(self, request, links):
        logger.debug("Links extracted %s (%d)", request.url, len(links))
        for link in links:
            logger.debug("URL: %s", link.url)
        self.states.set_states(links)
        self.strategy.links_extracted(request, links)
        self.states.update_cache(links)

    def on_request_error(self, request, error):
        logger.debug("Page error %s (%s)", request.url, error)
        self.states.set_states(request)
        self.strategy.page_error(request, error)
        self.states.update_cache(request)

    def set_process_info(self, process_info):
        self.process_info = process_info


class StrategyWorker(StatsExportMixin, BaseStrategyWorker):
    """Main strategy worker class with useful extensions.

    The additional features are provided by using mixin classes:
     - sending crawl stats to message bus
     """
    def get_stats_tags(self, settings, *args, **kwargs):
        return {'source': 'sw', 'partition_id': settings.get('SCORING_PARTITION_ID')}


def setup_environment():
    parser = ArgumentParser(description="Frontera strategy worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--strategy', type=str,
                        help='Crawling strategy class path')
    parser.add_argument('--partition-id', type=int,
                        help="Instance partition id.")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen.")
    parser.add_argument('--args', '-a', nargs='*', type=str, help="Optional arguments for crawling strategy, "
                                                                  "in a form of key=value separated with space")
    parser.add_argument('--add-seeds', action='store_true', help="Run in add seeds mode. Worker finishes after running "
                                                                 "of strategy add_seeds method")
    parser.add_argument('--seeds-url', type=str, help="Seeds url. S3 and native urlopen schemas are currently "
                                                      "supported, implies add seeds run mode")
    args = parser.parse_args()
    settings = Settings(module=args.config)
    strategy_classpath = args.strategy if args.strategy else settings.get('CRAWLING_STRATEGY')
    if not strategy_classpath:
        raise ValueError("Couldn't locate strategy class path. Please supply it either using command line option or "
                         "settings file.")
    strategy_class = load_object(strategy_classpath)

    partition_id = args.partition_id if args.partition_id is not None else settings.get('SCORING_PARTITION_ID')
    if partition_id >= settings.get('SPIDER_LOG_PARTITIONS') or partition_id < 0:
        raise ValueError("Partition id (%d) cannot be less than zero or more than SPIDER_LOG_PARTITIONS." %
                         partition_id)
    settings.set('SCORING_PARTITION_ID', partition_id)

    strategy_args = {}
    if args.args:
        for arg in args.args:
            key, _, value = arg.partition("=")
            strategy_args[key] = value if value else None

    logging_config_path = settings.get("LOGGING_CONFIG")
    if logging_config_path and exists(logging_config_path):
        fileConfig(logging_config_path, disable_existing_loggers=False)
    else:
        logging.basicConfig(level=args.log_level)
        logger.setLevel(args.log_level)
        logger.addHandler(CONSOLE)

    return settings, strategy_class, args.add_seeds, strategy_args, args.seeds_url


if __name__ == '__main__':
    settings, strategy_class, is_add_seeds_mode, strategy_args, seeds_url = setup_environment()
    worker = StrategyWorker(settings, strategy_class, strategy_args, is_add_seeds_mode)
    server = WorkerJsonRpcService(worker, settings)
    server.start_listening()
    worker.run(seeds_url)
