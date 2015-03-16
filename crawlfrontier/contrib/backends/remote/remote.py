import time

from codecs import KafkaJSONEncoder, KafkaJSONDecoder

from kafka import KafkaClient, SimpleConsumer, SimpleProducer
from kafka.common import BrokerResponseError

from crawlfrontier import Backend, Settings
from crawlfrontier.core.models import Request


class TestManager(object):
    class Nothing(object):
        pass

    def __init__(self):
        def log(msg):
            print "Test Manager: ", msg

        self.logger = TestManager.Nothing()
        self.settings = Settings()
        self.logger.backend = TestManager.Nothing()
        for log_level in (
                'info'
                'debug',
                'warning',
                'error'):
            setattr(self.logger.backend, log_level, log)


class KafkaBackend(Backend):
    def __init__(self, manager):
        self._manager = manager
        settings = manager.settings

        # Kafka connection parameters
        self._server = settings.get('KAFKA_SERVER', "localhost:9092")
        self._topic_todo = settings.get('KAFKA_TOPIC_TODO', "frontier-todo")
        self._topic_done = settings.get('KAFKA_TOPIC_DONE', "frontier-done")
        self._group = settings.get('KAFKA_GROUP', "scrapy-crawler")
        self._get_timeout = float(settings.get('KAFKA_GET_TIMEOUT', 5.0))
        self._partition_id = settings.get('KAFKA_PARTITION_ID')

        # Kafka setup
        self._conn = KafkaClient(self._server)
        self._prod = None
        self._cons = None

        self._connect_consumer()
        self._connect_producer()

        self._encoder = KafkaJSONEncoder(manager.request_model)
        self._decoder = KafkaJSONDecoder(manager.request_model, manager.response_model)
                
    def _connect_producer(self):
        """If producer is not connected try to connect it now.

        :returns: bool -- True if producer is connected
        """        
        if self._prod is None:
            try:
                self._prod = SimpleProducer(self._conn)
            except BrokerResponseError:
                self._prod = None        
                if self._manager is not None:
                    self._manager.logger.backend.warning(
                        "Could not connect producer to Kafka server")
                return False

        return True

    def _connect_consumer(self):
        """If consumer is not connected try to connect it now.

        :returns: bool -- True if consumer is connected
        """
        if self._cons is None:
            try:
                self._cons = SimpleConsumer(self._conn, self._group, self._topic_todo, partitions=[self._partition_id])
            except BrokerResponseError:
                self._cons = None
                if self._manager is not None:
                    self._manager.logger.backend.warning(
                        "Could not connect consumer to Kafka server")
                return False

        return True

    @classmethod
    def from_manager(clas, manager):
        return clas(manager)

    def frontier_start(self):
        if self._connect_consumer():
            self._manager.logger.backend.info(
                "Successfully connected consumer to " + self._topic_todo)
        else:
            self._manager.logger.backend.warning(
                "Could not connect consumer to {0}. I will try latter.".format(
                    self._topic_todo))

    def frontier_stop(self):        
        # flush everything if a batch is incomplete
        self._prod.stop()

    def _send_message(self, encoded_message, fail_wait_time=1.0, max_tries=5):
        start = time.clock()
        success = False
        if self._connect_producer():
            n_tries = 0
            while not success and n_tries < max_tries:
                try:
                    self._prod.send_messages(self._topic_done, encoded_message)
                    success = True
                except BrokerResponseError:
                    n_tries += 1
                    if self._manager is not None:
                        self._manager.logger.backend.warning(
                            "Could not send message. Try {0}/{1}".format(
                                n_tries, max_tries)
                        )

                    time.sleep(fail_wait_time)

        self._manager.logger.backend.debug("_send_message: {0}".format(time.clock() - start))
        return success

    def add_seeds(self, seeds):
        self._send_message(self._encoder.encode_add_seeds(seeds))

    def page_crawled(self, response, links):
        self._send_message(self._encoder.encode_page_crawled(response, links))
            
    def request_error(self, page, error):
        self._send_message(self._encoder.encode_request_error(page, error))

    def get_next_requests(self, max_n_requests):
        start = time.clock()
        requests = []

        if not self._connect_consumer():
            return []

        try:
            success = False
            for offmsg in self._cons.get_messages(
                    max_n_requests,
                    timeout=self._get_timeout):
                success = True
                try:
                    request = self._decoder.decode_request(offmsg.message.value)
                    requests.append(request)
                except ValueError:
                    self._manager.logger.backend.warning(
                        "Could not decode {0} message: {1}".format(
                            self._topic_todo,
                            offmsg.message.value))
            if not success:
                self._manager.logger.backend.warning(
                    "Timeout ({0} seconds) while trying to get {1} requests".format(
                        self._get_timeout,
                        max_n_requests)
                )
        except BrokerResponseError:
            self._manager.logger.backend.warning(
                "Could not connect consumer to " + self._topic_todo)
            
        self._manager.logger.backend.debug("get_next_requests: {0}".format(time.clock() - start))
        return requests
