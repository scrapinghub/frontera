from logging import getLogger
from traceback import format_tb

from twisted.internet.task import LoopingCall

from frontera.utils.misc import load_object, utc_timestamp

logger = getLogger("messagebus.stats")


class StatsExportMixin(object):
    """Extending Frontera worker's logic by sending stats to message bus.

    This is a lightweight mixin class to be used with a base worker classes
    by sending stats to message bus if configured. The mixin also allows
    you to define your custom logic for get_stats_tags() logic in your child
    classes to store a dictionary with tags as a part of your metrics.
    """
    STATS_PREFIXES = ['consumed', 'pushed', 'dropped']

    def __init__(self, settings, *args, **kwargs):
        super(StatsExportMixin, self).__init__(settings, *args, **kwargs)
        message_bus = load_object(settings.get('MESSAGE_BUS'))(settings)
        stats_log = message_bus.stats_log()
        # FIXME can be removed after implementing stats_log for ZeroMQ bus
        if not stats_log:
            return
        self.stats_producer = stats_log.producer()
        self._stats_tags = self.get_stats_tags(settings, *args, **kwargs)
        self._stats_interval = settings.get('STATS_LOG_INTERVAL', 60)
        self._export_stats_task = LoopingCall(self.export_stats)

    def run(self, *args, **kwargs):

        def errback_export_stats(failure):
            logger.exception(failure.value)
            if failure.frames:
                logger.critical(str("").join(format_tb(failure.getTracebackObject())))
            self._export_stats_task.start(interval=self._stats_interval)\
                                   .addErrback(errback_export_stats)

        if self.stats_producer:
            self._export_stats_task.start(interval=self._stats_interval)\
                                   .addErrback(errback_export_stats)
        super(StatsExportMixin, self).run(*args, **kwargs)

    def get_stats_tags(self, *args, **kwargs):
        """Get a tags dictionary for the metrics.

        Default implementation expects that this method will provide:
        - 'source' - source type of the metric, one of ['sw', 'dbw', 'spider']
        - 'partition_id' (optionally) - specific partition id
        """
        raise NotImplemented("Please define the method in a child class")

    @property
    def _stats_key_prefix(self):
        """Build key prefix based on the given tags.

        Default implementation of the method relies on get_stats_tags() logic,
        and existence of 'source'/'partition_id' tags.
        """
        prefix = self._stats_tags.get('source')
        if 'partition_id' in self._stats_tags:
            prefix += '-{}'.format(self._stats_tags.get('partition_id'))
        return prefix

    def export_stats(self):
        """Export crawl stats to message bus.

        Message is formed in the following way:
        - key: a prefix from _stats_key_prefix() + stats timestamp
        - value: a stats dictionary packed with self._encoder
        """
        stats = self.get_stats()
        if not stats:
            return
        stats_key = '{}-{}'.format(self._stats_key_prefix, stats['_timestamp'])
        # self._encoder is defined as a part of worker initialization
        encoded_msg = self._encoder.encode_stats(stats)
        self.stats_producer.send(stats_key, encoded_msg)
        logger.debug("Sent stats for {} to message bus: {}".format(stats_key, stats))

    def get_stats(self):
        """Return default stats with a timestamp.

        It's useful to have a default implementation of the method because both
        strategy and db worker store stats this way, though this logic could be
        modified in a child class to redefine/transform stats data.
        """
        # report only stats with given prefixes, no need to push all of them
        stats = {stats_key: self.stats[stats_key]
                 for stats_key in self.stats
                 if stats_key.split('_', 1)[0] in self.STATS_PREFIXES}
        stats.update(self.backend.get_stats() or {})
        if not stats:
            return
        stats['_timestamp'] = utc_timestamp()
        stats['_tags'] = self._stats_tags
        return stats