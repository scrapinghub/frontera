import logging
from traceback import format_tb

from scrapy import signals
from scrapy.exceptions import NotConfigured
from twisted.internet.task import LoopingCall

from frontera.contrib.scrapy.settings_adapter import ScrapySettingsAdapter
from frontera.utils.misc import utc_timestamp, load_object


logger = logging.getLogger(__name__)

# scrapy stats ignored by the exporter by default
STATS_DEFAULT_BLACKLIST = [
    "start_time",
]


class StatsExporterToMessageBus(object):
    """Export crawl stats to message bus."""

    def __init__(self, crawler):
        settings = ScrapySettingsAdapter(crawler.settings)
        self.partition_id = settings.get('SPIDER_PARTITION_ID')
        # XXX this can be improved later by reusing spider's producer
        # (crawler->engine->slot->scheduler->frontier->manager-> backend->_producer)
        # but the topic is hard-coded in the current scheme, so it requires some
        # preliminary changes in Frontera itself.
        message_bus = load_object(settings.get('MESSAGE_BUS'))(settings)
        stats_log = message_bus.stats_log()
        if not stats_log:
            raise NotConfigured
        self.stats_producer = stats_log.producer()
        self._stats_interval = settings.get('STATS_LOG_INTERVAL', 60)
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path + ".Encoder")
        self._stats_encoder = encoder_cls(request_model=None)  # no need to encode requests
        self._export_stats_task = None

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler)
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(obj.spider_closed, signal=signals.spider_closed)
        return obj

    def spider_opened(self, spider):

        def errback_export_stats(failure):
            logger.exception(failure.value)
            if failure.frames:
                logger.critical(str("").join(format_tb(failure.getTracebackObject())))
            self._export_stats_task.start(self._stats_interval)\
                                   .addErrback(errback_export_stats)

        self._export_stats_task = LoopingCall(self.export_stats, spider)
        self._export_stats_task.start(self._stats_interval)\
                               .addErrback(errback_export_stats)

    def spider_closed(self, spider):
        if self._export_stats_task:
            self._export_stats_task.stop()
            self._export_stats_task = None
            self.stats_producer.flush()
            self.stats_producer.close()

    def export_stats(self, spider):
        all_stats = spider.crawler.stats.get_stats()
        stats = {key: all_stats[key] for key in all_stats
                 if key not in STATS_DEFAULT_BLACKLIST}
        if not stats:
            return  # no need to send empty stats
        stats['_timestamp'] = utc_timestamp()
        stats['_tags'] = {'source': 'spider', 'partition_id': self.partition_id}
        key = 'spider-{}-{}'.format(self.partition_id, stats['_timestamp'])
        encoded_msg = self._stats_encoder.encode_stats(stats)
        self.stats_producer.send(key, encoded_msg)
        logger.debug("Sent spider stats to message bus: %s", stats)
