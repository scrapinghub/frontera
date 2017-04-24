from __future__ import absolute_import

import logging

import six

from w3lib.util import to_bytes, to_native_str

from frontera.contrib.backends.memory import MemoryStates
from frontera.utils.misc import chunks

from .models import DeclarativeBase
from .utils import retry_and_rollback


class States(MemoryStates):

    def __init__(self, session_cls, model_cls, cache_size_limit):
        super(States, self).__init__(cache_size_limit)
        self.session = session_cls()
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['states']
        self.logger = logging.getLogger("sqlalchemy.states")

    @retry_and_rollback
    def frontier_stop(self):
        self.flush()
        self.session.close()

    @retry_and_rollback
    def fetch(self, fingerprints):
        to_fetch = [to_native_str(f) for f in fingerprints if f not in self._cache]

        self.logger.debug("cache size %s", len(self._cache))
        self.logger.debug("to fetch %d from %d", len(to_fetch), len(fingerprints))

        for chunk in chunks(to_fetch, 128):
            for state in self.session.query(self.model).filter(self.model.fingerprint.in_(chunk)):
                self._cache[to_bytes(state.fingerprint)] = state.state

    @retry_and_rollback
    def flush(self, force_clear=False):
        for fingerprint, state_val in six.iteritems(self._cache):
            state = self.model(fingerprint=to_native_str(fingerprint), state=state_val)

            self.session.merge(state)

        self.session.commit()
        self.logger.debug("State cache has been flushed.")

        super(States, self).flush(force_clear)
