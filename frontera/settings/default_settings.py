from datetime import timedelta


AUTO_START = True
BACKEND = 'frontera.contrib.backends.memory.FIFO'
CANONICAL_SOLVER = 'frontera.contrib.canonicalsolvers.Basic'
DELAY_ON_EMPTY = 5.0
DOMAIN_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'

HBASE_THRIFT_HOST = 'localhost'
HBASE_THRIFT_PORT = 9090
HBASE_NAMESPACE = 'crawler'
HBASE_DROP_ALL_TABLES = False
HBASE_METADATA_TABLE = 'metadata'
HBASE_USE_SNAPPY = False
HBASE_USE_FRAMED_COMPACT = False
HBASE_BATCH_SIZE = 9216
HBASE_STATE_CACHE_SIZE_LIMIT = 3000000
HBASE_QUEUE_TABLE = 'queue'
KAFKA_GET_TIMEOUT = 5.0
KAFKA_CODEC_LEGACY = "none"
MAX_NEXT_REQUESTS = 64
MAX_REQUESTS = 0
MESSAGE_BUS = 'frontera.contrib.messagebus.zeromq.MessageBus'
MIDDLEWARES = [
    'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
]
NEW_BATCH_DELAY = 30.0
OVERUSED_SLOT_FACTOR = 5.0
QUEUE_HOSTNAME_PARTITIONING = False
REQUEST_MODEL = 'frontera.core.models.Request'
RESPONSE_MODEL = 'frontera.core.models.Response'

SCORING_PARTITION_ID = 0
SCORING_LOG_CONSUMER_BATCH_SIZE = 512
SPIDER_LOG_CONSUMER_BATCH_SIZE = 512
SPIDER_LOG_PARTITIONS = 1
SPIDER_FEED_PARTITIONS = 1
SPIDER_PARTITION_ID = 0
SQLALCHEMYBACKEND_CACHE_SIZE = 10000
SQLALCHEMYBACKEND_CLEAR_CONTENT = True
SQLALCHEMYBACKEND_DROP_ALL_TABLES = True
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///:memory:'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_MODELS = {
    'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
    'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
    'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel'
}
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=1)
STATE_CACHE_SIZE = 1000000
STORE_CONTENT = False
TEST_MODE = False
TLDEXTRACT_DOMAIN_INFO = False
URL_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'

ZMQ_ADDRESS = '127.0.0.1'
ZMQ_BASE_PORT = 5550

LOGGING_CONFIG = 'logging.conf'