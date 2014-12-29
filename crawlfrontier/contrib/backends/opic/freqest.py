"""
A frequenecy estimator takes into account how many times a page has been
observed changed or unchanged and what points in time, to estimate the
refresing rate of the page
"""
import time

from abc import ABCMeta, abstractmethod

import updatesdb


class FreqEstimatorInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add(self, page_id, initial_freq):
        """Add initial frequency estimation for page_id

        page_id      -- An string which identifies the page
        initial_freq -- Initial frequency estimation (Hz)
        """
        pass

    @abstractmethod
    def refresh(self, page_id, updated):
        """Add new refresh information for page_id

        updated -- A boolean indicating if the page has changed
        """
        pass

    @abstractmethod
    def delete(self, page_id):
        """Stop tracking page_id"""
        pass

    @abstractmethod
    def frequency(self, page_id):
        """Return the estimated refresh frequency for the page"""
        pass

    @abstractmethod
    def close(self):
        """Persist or flush all necesary information"""
        pass


class Simple(FreqEstimatorInterface):
    """
    The simple estimator just computes the frequency as the total
    number of updates divided by the total observation time
    """
    def __init__(self, db=None, clock=None):
        self._db = db or updatesdb.SQLite()
        self._clock = clock or time.time

    def add(self, page_id, initial_freq):
        self._db.add(
            page_id,
            self._clock() - 1.0/initial_freq,
            1
        )

    def refresh(self, page_id, updated):
        if updated:
            self._db.increment(page_id, 1)

    def delete(self, page_id):
        self._db.delete(page_id)

    def frequency(self, page_id):
        (start_time, updates) = self._db.get(page_id) or (None, None)

        if start_time:
            return float(updates)/(self._clock() - start_time)
        else:
            return 0.0

    def close(self):
        self._db.close()
