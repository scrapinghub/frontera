from __future__ import absolute_import

import logging

from calendar import timegm
from datetime import datetime
from time import sleep


logger = logging.getLogger(__name__)


def retry_and_rollback(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5

        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception as exc:
                logger.exception(exc)
                self.session.rollback()

                sleep(5)

                tries -= 1

                if tries > 0:
                    logger.info("Tries left %d", tries)

                    continue
                else:
                    raise exc

    return func_wrapper


def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())
