# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import logging
import threading
from traceback import format_stack
from signal import signal, SIGUSR1
from collections import defaultdict
from argparse import ArgumentParser
from logging.config import fileConfig

import six
from twisted.internet import reactor, task, defer
from twisted.internet.defer import Deferred

from frontera.settings import Settings
from frontera.utils.misc import load_object
from frontera.logger.handlers import CONSOLE
from frontera.exceptions import NotConfigured
from frontera.core.manager import WorkerFrontierManager
from frontera.worker.server import WorkerJsonRpcService
from frontera.utils.ossignal import install_shutdown_handlers
from frontera.worker.stats import StatsExportMixin

from .components.incoming_consumer import IncomingConsumer
from .components.scoring_consumer import ScoringConsumer
from .components.batch_generator import BatchGenerator


ALL_COMPONENTS = [ScoringConsumer, IncomingConsumer, BatchGenerator]
LOGGING_TASK_INTERVAL = 30

logger = logging.getLogger("db-worker")


class Slot(object):
    """Slot component to manage worker components.

    Slot is responsible for scheduling all the components, modify its behaviour
    and stop them gracefully on worker's discretion.
    """
    def __init__(self, worker, settings, **kwargs):
        # single event to stop all the components at once
        self.stop_event = threading.Event()
        self.components = self._load_components(worker, settings, **kwargs)
        self._setup_managing_batches()
        self._deferred = None

    def _load_components(self, worker, settings, **kwargs):
        # each component is stored as (cls, instance) pair
        components = {}
        for cls in ALL_COMPONENTS:
            try:
                component = cls(worker, settings, stop_event=self.stop_event, **kwargs)
            except NotConfigured:
                logger.info("Component {} is disabled".format(cls.NAME))
            else:
                components[cls] = component
        if not components:
            raise NotConfigured("No components to run, please check your input args")
        return components

    def schedule(self):
        # component.schedule() function must return None or Deferred
        scheduled = [component.schedule() for component in self.components.values()]
        deferred = [result for result in scheduled if isinstance(result, Deferred)]
        self._deferred = defer.DeferredList(deferred) if deferred else None

    def stop(self):
        """Set stop flag and return a defferred connected with all running threads."""
        self.stop_event.set()
        return self._deferred if self._deferred else None

    def close(self):
        for component in self.components.values():
            component.close()

    # Additional functions to manage specific components

    # XXX do we actually use this feature to disable/enable new batches?
    # it should be easier to just stop the batchgen component and start it again when needed

    def _setup_managing_batches(self):
        """Save batch-gen specific event to disable/enable it via RPC calls."""
        batchgen = self.components.get(BatchGenerator)
        self.batches_disabled_event = batchgen.disabled_event if batchgen else None

    def manage_new_batches(self, enable):
        if self.batches_disabled_event:
            self.batches_disabled_event.clear() if enable else self.batches_disabled_event.set()


class BaseDBWorker(object):
    """Base database worker class."""

    def __init__(self, settings, no_batches, no_incoming, no_scoring, **kwargs):

        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.message_bus = messagebus(settings)

        self._manager = WorkerFrontierManager.from_settings(settings, db_worker=True)
        self.backend = self._manager.backend

        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        self._encoder = encoder_cls(self._manager.request_model)
        self._decoder = decoder_cls(self._manager.request_model, self._manager.response_model)

        slot_kwargs = {'no_batches': no_batches,
                       'no_incoming': no_incoming,
                       'no_scoring': no_scoring}
        slot_kwargs.update(**kwargs)
        self.slot = Slot(self, settings, **slot_kwargs)

        self.stats = defaultdict(int)
        self.job_id = 0
        self._logging_task = task.LoopingCall(self.log_status)

    def run(self):
        def debug(sig, frame):
            logger.critical("Signal received: printing stack trace")
            logger.critical(str("").join(format_stack(frame)))

        self.slot.schedule()
        self._logging_task.start(LOGGING_TASK_INTERVAL)
        install_shutdown_handlers(self._handle_shutdown)
        signal(SIGUSR1, debug)
        reactor.run(installSignalHandlers=False)

    # Auxiliary methods

    def update_stats(self, replacements=None, increments=None):
        if replacements:
            for key, value in replacements.items():
                self.stats[key] = value
        if increments:
            for key, value in increments.items():
                self.stats[key] += value

    def set_process_info(self, process_info):
        self.process_info = process_info

    def log_status(self):
        for k, v in six.iteritems(self.stats):
            logger.info("%s=%s", k, v)

    # Graceful shutdown

    def _handle_shutdown(self, signum, _):
        def call_shutdown():
            d = self.stop_tasks()
            reactor.callLater(0, d.callback, None)

        logger.info("Received shutdown signal %d, shutting down gracefully.", signum)
        reactor.callFromThread(call_shutdown)

    def stop_tasks(self):
        logger.info("Stopping periodic tasks.")
        self._logging_task.stop()

        d = Deferred()
        d.addBoth(self._stop_slot)
        d.addBoth(self._close_slot)
        d.addBoth(self._perform_shutdown)
        d.addBoth(self._stop_reactor)
        return d

    def _stop_slot(self, _=None):
        logger.info("Stopping DB worker slot.")
        return self.slot.stop()

    def _close_slot(self, _=None):
        logger.info('Closing DB worker slot resources.')
        self.slot.close()

    def _perform_shutdown(self, _=None):
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def _stop_reactor(self, _=None):
        logger.info("Stopping reactor.")
        try:
            reactor.stop()
        except RuntimeError:  # raised if already stopped or in shutdown stage
            pass


class DBWorker(StatsExportMixin, BaseDBWorker):
    """Main database worker class with useful extensions.

    The additional features are provided by using mixin classes:
     - sending crawl stats to message bus
     """
    def get_stats_tags(self, settings, no_batches, no_incoming, no_scoring, **kwargs):
        if no_batches and no_scoring:
            db_worker_type = 'linksdb'
        elif no_batches and no_incoming:
            db_worker_type = 'scoring'
        elif no_incoming and no_scoring:
            db_worker_type = 'batchgen'
        else:
            logger.warning("Can't identify DB worker type "
                           "(no-scoring {}, no-batches {}, no-incoming {})"
                           .format(no_scoring, no_batches, no_incoming))
            db_worker_type = 'none'
        tags = {'source': 'dbw-{}'.format(db_worker_type)}
        # add mesos task id as a tag if running via marathon
        mesos_task_id = os.environ.get('MESOS_TASK_ID')
        if mesos_task_id:
            tags['mesos_task_id'] = mesos_task_id
        return tags


if __name__ == '__main__':
    parser = ArgumentParser(description="Frontera DB worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables generation of new batches.')
    parser.add_argument('--no-incoming', action='store_true',
                        help='Disables spider log processing.')
    parser.add_argument('--no-scoring', action='store_true',
                        help='Disables scoring log processing.')
    parser.add_argument('--partitions', type=int, nargs='*',
                        help='Optional partitions range for batch generator')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import.')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL.")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen.")
    args = parser.parse_args()

    settings = Settings(module=args.config)
    if args.port:
        settings.set("JSONRPC_PORT", [args.port])

    logging_config_path = settings.get("LOGGING_CONFIG")
    if logging_config_path and os.path.exists(logging_config_path):
        fileConfig(logging_config_path, disable_existing_loggers=False)
    else:
        logging.basicConfig(level=args.log_level)
        logger.setLevel(args.log_level)
        logger.addHandler(CONSOLE)

    worker = DBWorker(settings, args.no_batches, args.no_incoming,
                      args.no_scoring, partitions=args.partitions)
    server = WorkerJsonRpcService(worker, settings)
    server.start_listening()
    worker.run()

