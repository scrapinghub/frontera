# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
import logging
import threading

from twisted.internet import reactor, task, threads

from frontera.exceptions import NotConfigured
from frontera.utils.async import CallLaterOnce


class DBWorkerBaseComponent(object):

    NAME = None

    def __init__(self, worker, settings):
        self.worker = worker
        self.settings = settings
        self.logger = logging.getLogger('db-worker.{}'.format(self.NAME))

    def schedule(self, delay=0):
        """Schedule component start with optional delay.
        The function must return None or Deferred.
        """
        raise NotImplementedError

    def run(self):
        """Iteration logic, must be implemented in a subclass."""
        raise NotImplementedError

    def stop(self):
        """Optional stop logic called by the reactor thread."""


class DBWorkerPeriodicComponent(DBWorkerBaseComponent):

    def __init__(self, worker, settings, *args, **kwargs):
        super(DBWorkerPeriodicComponent, self).__init__(worker, settings)
        self.periodic_task = CallLaterOnce(self.run_with_callback)
        self.periodic_task.setErrback(self.run_errback)

    def schedule(self, delay=0):
        self.logger.info('Periodic scheduled!')
        self.periodic_task.schedule(delay)

    def run_with_callback(self):
        self.logger.info('Run with callback!')
        self.run()
        self.periodic_task.schedule()
        self.logger.info('Scheduled again!')

    def run_errback(self, failure):
        self.logger.exception(failure.value)
        self.periodic_task.schedule()

    def stop(self):
        self.periodic_task.cancel()


class DBWorkerThreadComponent(DBWorkerBaseComponent):
    """Base class for DB worker component running in a separate thread.

    The class defines a single interface for DB worker components: you should
    mainly implement only .run() method representing a single component iteration.
    """

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        self.stop_event = stop_event
        self.run_backoff = 0  # replace it with a proper value in subclass

    def schedule(self):
        return threads.deferToThread(self.loop)

    def loop(self):
        """Main entrypoint for the thread running loop."""
        while not self.stop_event.is_set():
            self.logger.info('Thread iteration!')
            try:
                self.run()
            except Exception as exc:
                self.logger.exception('Exception in the main loop')
            if self.run_backoff:
                self.logger.debug('Sleep for {} seconds before next run()'
                                  .format(self.run_backoff))
                time.sleep(self.run_backoff)
        self.logger.debug("Main loop was stopped")

    def run(self):
        """Logic for single iteration of the component."""
        raise NotImplementedError

    def update_stats(self, **kwargs):
        """Helper to update worker stats."""
        threads.blockingCallFromThread(reactor, self.worker.update_stats, **kwargs)
