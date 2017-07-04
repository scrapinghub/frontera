from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, \
    BaseScoringLogStream, BaseSpiderFeedStream, BaseStreamProducer


class Consumer(BaseStreamConsumer):

    def __init__(self):
        self.messages = []
        self.offset = None

    def put_messages(self, messages=[]):
        self.messages += messages

    def get_messages(self, timeout=0, count=1):
        lst = []
        for _ in range(count):
            if self.messages:
                lst.append(self.messages.pop())
            else:
                break
        return lst

    def _set_offset(self, offset):
        self.offset = offset

    def get_offset(self, partition_id):
        return self.offset


class Producer(BaseStreamProducer):

    def __init__(self):
        self.messages = []
        self.offset = 0

    def send(self, key, *messages):
        self.messages += messages

    def flush(self):
        pass

    def get_offset(self, partition_id):
        return self.offset

    def partition(self, key):
        return 0


class ScoringLogStream(BaseScoringLogStream):

    def __init__(self, messagebus):
        pass

    def producer(self):
        return Producer()

    def consumer(self):
        return Consumer()


class SpiderLogStream(BaseSpiderLogStream):

    def __init__(self, messagebus):
        pass

    def producer(self):
        return Producer()

    def consumer(self, partition_id, type):
        return Consumer()


class SpiderFeedStream(BaseSpiderFeedStream):

    def __init__(self, messagebus):
        self._producer = Producer()
        self.max_next_requests = messagebus.max_next_requests
        self.partitions_offset = {}
        for partition_id in messagebus.spider_feed_partitions:
            self.partitions_offset[partition_id] = 0

    def producer(self):
        return self._producer

    def consumer(self, partition_id):
        return Consumer()

    def available_partitions(self):
        partitions = []
        for partition_id, last_offset in self.partitions_offset.items():
            lag = self._producer.get_offset(partition_id) - last_offset
            if lag < self.max_next_requests or last_offset == 0:
                partitions.append(partition_id)
        return partitions

    def set_spider_offset(self, partition_id, offset):
        self.partitions_offset[partition_id] = offset

class FakeMessageBus(BaseMessageBus):

    def __init__(self, settings):
        self.spider_log_partitions = [i for i in range(settings.get('SPIDER_LOG_PARTITIONS'))]
        self.spider_feed_partitions = [i for i in range(settings.get('SPIDER_FEED_PARTITIONS'))]
        self.max_next_requests = settings.get('MAX_NEXT_REQUESTS')

    def spider_log(self):
        return SpiderLogStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)
