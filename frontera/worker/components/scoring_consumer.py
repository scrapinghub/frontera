# -*- coding: utf-8 -*-
from __future__ import absolute_import

from time import asctime

from frontera.exceptions import NotConfigured
from frontera.core.components import DistributedBackend
from . import DBWorkerPeriodicComponent


class ScoringConsumer(DBWorkerPeriodicComponent):
    """Component to get data from scoring log and send it to backend queue."""

    NAME = 'scoring'

    def __init__(self, worker, settings, stop_event, no_scoring=False, **kwargs):
        super(ScoringConsumer, self).__init__(worker, settings, stop_event, **kwargs)
        if no_scoring:
            raise NotConfigured('ScoringConsumer is disabled with --no-scoring')
        if not isinstance(worker.backend, DistributedBackend):
            raise NotConfigured('Strategy is disabled for non-distributed backend')

        scoring_log = worker.message_bus.scoring_log()
        self.scoring_log_consumer = scoring_log.consumer()
        self.scoring_log_consumer_batch_size = settings.get('SCORING_LOG_CONSUMER_BATCH_SIZE')
        self.backend_queue = worker.backend.queue

    def run(self):
        consumed, seen, batch = 0, set(), []
        for m in self.scoring_log_consumer.get_messages(
                count=self.scoring_log_consumer_batch_size):
            try:
                msg = self.worker._decoder.decode(m)
            except (KeyError, TypeError):
                self.logger.exception("Decoding error")
                continue
            else:
                if msg[0] == 'update_score':
                    _, request, score, schedule = msg
                    if request.meta[b'fingerprint'] not in seen:
                        batch.append((request.meta[b'fingerprint'],
                                      score, request, schedule))
                        seen.add(request.meta[b'fingerprint'])
                elif msg[0] == 'new_job_id':
                    self.worker.job_id = msg[1]
            finally:
                consumed += 1
        self.backend_queue.schedule(batch)
        self.worker.update_stats(increments={'consumed_scoring_since_start': consumed},
                                 replacements={'last_consumed_scoring': consumed,
                                               'last_consumption_run_scoring': asctime()})

    def close(self):
        self.scoring_log_consumer.close()
