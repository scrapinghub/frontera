from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, \
    BaseScoringLogStream, BaseSpiderFeedStream


class Consumer(BaseStreamConsumer):

    def __init__(self):
        self.messages = []

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

    def get_offset(self):
        pass


class Producer(object):

    def __init__(self):
        self.messages = []

    def send(self, key, *messages):
        self.messages += messages

    def flush(self):
        pass

    def get_offset(self, partition_id):
        pass


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
        pass

    def producer(self):
        return Producer()

    def consumer(self, partition_id):
        return Consumer()

    def available_partitions(self):
        return set([0])


class FakeMessageBus(BaseMessageBus):

    def __init__(self, settings):
        self.spider_log_partitions = settings.get('SPIDER_LOG_PARTITIONS')
        self.spider_feed_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        self.max_next_requests = settings.get('MAX_NEXT_REQUESTS')

    def spider_log(self):
        return SpiderLogStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)
