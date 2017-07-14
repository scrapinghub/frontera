# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
import logging
import threading

from frontera.exceptions import NotConfigured

from twisted.internet import reactor, task, threads


class DBWorkerComponent(object):
    """Base class for DB worker component.

    The class defines a single interface for DB worker components: you should
    mainly implement only .run() method representing a single component iteration.
    """

    NAME = None

    def __init__(self, worker, settings, stop_event, *args, **kwargs):
        self.worker = worker
        self.settings = settings
        self.stop_event = stop_event
        self.logger = logging.getLogger('db-worker.{}'.format(self.NAME))
        self.run_backoff = 0  # replace it with a proper value in subclass

    def schedule(self):
        return threads.deferToThread(self.loop)

    def loop(self):
        """Main entrypoint for the thread running loop."""
        while not self.stop_event.is_set():
            try:
                self.run()
            except Exception as exc:
                self.logger.exception('Exception in the main loop')
            if self.run_backoff:
                self.logger.debug('Sleep for {} seconds before next run()'
                                  .format(self.run_backoff))
                time.sleep(self.run_backoff)
        self.logger.debug("Main loop was stopped")
        self.close()

    def run(self):
        """Logic for single iteration of the component."""
        raise NotImplementedError

    def update_stats(self, **kwargs):
        """Helper to update worker stats."""
        threads.blockingCallFromThread(reactor, self.worker.update_stats, **kwargs)

    def close(self):
        """Optional method to do some clean-up before exiting main loop."""
