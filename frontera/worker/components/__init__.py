# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time

import logging
from frontera.utils.twisted_helpers import CallLaterOnce
from twisted.internet import reactor, threads


class DBWorkerBaseComponent(object):

    NAME = None

    def __init__(self, worker, settings, stop_event):
        self.worker = worker
        self.settings = settings
        self.stop_event = stop_event
        self.logger = logging.getLogger('db-worker.{}'.format(self.NAME))

    def schedule(self, delay=0):
        """Schedule component start with optional delay.
        The function must return None or Deferred.
        """
        raise NotImplementedError

    def run(self):
        """Iteration logic, must be implemented in a subclass."""
        raise NotImplementedError

    def close(self):
        """Optional cleanup logic when component loop is stopped."""


class DBWorkerPeriodicComponent(DBWorkerBaseComponent):

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        super(DBWorkerPeriodicComponent, self).__init__(worker, settings, stop_event)
        self.periodic_task = CallLaterOnce(self.run_and_reschedule)
        self.periodic_task.setErrback(self.run_errback)

    def schedule(self, delay=0):
        self.periodic_task.schedule(delay)

    def run_and_reschedule(self):
        if not self.stopped:
            self.run()
            self.periodic_task.schedule()

    def run_errback(self, failure):
        self.logger.error(failure.getTraceback())
        if not self.stopped:
            self.periodic_task.schedule()

    @property
    def stopped(self):
        return self.stop_event.is_set()


class DBWorkerThreadComponent(DBWorkerBaseComponent):
    """Base class for DB worker component running in a separate thread.

    The class defines a single interface for DB worker components: you should
    mainly implement only .run() method representing a single component iteration.
    """

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        super(DBWorkerThreadComponent, self).__init__(worker, settings, stop_event)
        self.run_backoff = 0  # replace it with a proper value in subclass

    def schedule(self):
        return threads.deferToThread(self.loop)

    def loop(self):
        """Main entrypoint for the thread running loop."""
        while not self.stop_event.is_set():
            try:
                is_backoff_needed = self.run()
            except Exception:
                self.logger.exception('Exception in the main loop')
            else:
                if is_backoff_needed and self.run_backoff:
                    delay_msg = 'Sleep for {} seconds before next run()'
                    self.logger.debug(delay_msg.format(self.run_backoff))
                    time.sleep(self.run_backoff)
        self.logger.debug("Main loop was stopped")

    def run(self):
        """Logic for single iteration of the component.

        The method must return True-ish value if backoff is needed between iteration.
        """
        raise NotImplementedError

    def update_stats(self, **kwargs):
        """Helper to update worker stats."""
        if reactor.running:
            reactor.callFromThread(self.worker.update_stats, **kwargs)
        else:
            # for testing purposes
            self.worker.update_stats(**kwargs)
