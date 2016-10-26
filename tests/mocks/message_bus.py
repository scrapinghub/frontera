from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, \
    BaseScoringLogStream, BaseSpiderFeedStream


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


class Producer(object):

    def __init__(self):
        self.messages = []
        self.offset = 0

    def send(self, key, *messages):
        self.messages += messages

    def flush(self):
        pass

    def get_offset(self, partition_id):
        return self.offset


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
        self.ready_partitions = set(messagebus.spider_feed_partitions)

    def producer(self):
        return Producer()

    def consumer(self, partition_id):
        return Consumer()

    def available_partitions(self):
        return self.ready_partitions

    def mark_ready(self, partition_id):
        self.ready_partitions.add(partition_id)

    def mark_busy(self, partition_id):
        self.ready_partitions.discard(partition_id)


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
