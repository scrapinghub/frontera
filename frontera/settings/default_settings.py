from __future__ import absolute_import
from datetime import timedelta


AUTO_START = True
BACKEND = 'frontera.contrib.backends.memory.MemoryDistributedBackend'
BC_MIN_REQUESTS = 64
BC_MIN_HOSTS = 24
BC_MAX_REQUESTS_PER_HOST = 128
CANONICAL_SOLVER = 'frontera.contrib.canonicalsolvers.Basic'
DELAY_ON_EMPTY = 5.0
DISCOVERY_MAX_PAGES = 100
DOMAIN_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'
DOMAIN_STATS_LOG_INTERVAL = 300

HBASE_THRIFT_HOST = 'localhost'
HBASE_THRIFT_PORT = 9090
HBASE_NAMESPACE = 'crawler'
HBASE_DROP_ALL_TABLES = False
HBASE_DOMAIN_METADATA_TABLE = 'domain_metadata'
HBASE_DOMAIN_METADATA_CACHE_SIZE = 1000
HBASE_DOMAIN_METADATA_BATCH_SIZE = 100
HBASE_METADATA_TABLE = 'metadata'
HBASE_STATES_TABLE = 'states'
HBASE_USE_SNAPPY = False
HBASE_USE_FRAMED_COMPACT = False
HBASE_BATCH_SIZE = 9216
HBASE_STATE_CACHE_SIZE_LIMIT = 3000000
HBASE_STATE_WRITE_LOG_SIZE = 15000
HBASE_QUEUE_TABLE = 'queue'
KAFKA_GET_TIMEOUT = 5.0
LOCAL_MODE = True
MAX_NEXT_REQUESTS = 64
MAX_REQUESTS = 0
MESSAGE_BUS = 'frontera.contrib.messagebus.zeromq.MessageBus'
MESSAGE_BUS_CODEC = 'frontera.contrib.backends.remote.codecs.msgpack'
MIDDLEWARES = [
    'frontera.contrib.middlewares.domain.DomainMiddleware',
    'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware'
]
NEW_BATCH_DELAY = 30.0
DOMAINS_BLACKLIST = None
OVERUSED_SLOT_FACTOR = 5.0
OVERUSED_MAX_PER_KEY = None
OVERUSED_KEEP_PER_KEY = 1000
OVERUSED_MAX_KEYS = None
OVERUSED_KEEP_KEYS = 100
QUEUE_HOSTNAME_PARTITIONING = False
REDIS_BACKEND_CODEC = 'frontera.contrib.backends.remote.codecs.msgpack'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REQUEST_MODEL = 'frontera.core.models.Request'
RESPONSE_MODEL = 'frontera.core.models.Response'

SCORING_PARTITION_ID = 0
SCORING_LOG_CONSUMER_BATCH_SIZE = 512
SPIDER_LOG_CONSUMER_BATCH_SIZE = 512
SPIDER_LOG_PARTITIONS = 1
SPIDER_FEED_PARTITIONS = 1
SPIDER_PARTITION_ID = 0
SQLALCHEMYBACKEND_CACHE_SIZE = 10000
SQLALCHEMYBACKEND_CLEAR_CONTENT = False
SQLALCHEMYBACKEND_DROP_ALL_TABLES = False
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///:memory:'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_MODELS = {
    'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
    'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
    'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel',
    'DomainMetadataModel': 'frontera.contrib.backends.sqlalchemy.models.DomainMetadataModel'
}
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=1)
STATE_CACHE_SIZE = 1000000
STATE_CACHE_SIZE_LIMIT = 0
STORE_CONTENT = False
STRATEGY = 'frontera.strategy.basic.BasicCrawlingStrategy'
STRATEGY_ARGS = {}
SW_FLUSH_INTERVAL = 300
TEST_MODE = False
TLDEXTRACT_DOMAIN_INFO = False
URL_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'
USER_AGENT = 'FronteraDiscoveryBot'

ZMQ_ADDRESS = '127.0.0.1'
ZMQ_BASE_PORT = 5550

LOGGING_CONFIG = 'logging.conf'

#--------------------------------------------------------
# Kafka
#--------------------------------------------------------

SPIDER_FEED_TOPIC = "frontier-todo"
SPIDER_LOG_TOPIC = "frontier-done"
SCORING_LOG_TOPIC = "frontier-score"
STATS_LOG_TOPIC = 'frontier-stats'

SPIDER_LOG_DBW_GROUP = "dbw-spider-log"
SPIDER_LOG_SW_GROUP = "sw-spider-log"
SCORING_LOG_DBW_GROUP = "dbw-scoring-log"
SPIDER_FEED_GROUP = "fetchers-spider-feed"
STATS_LOG_READER_GROUP = 'stats-reader-log'

KAFKA_CODEC = None
KAFKA_CERT_PATH = '/mnt/mesos/sandbox'
KAFKA_ENABLE_SSL = False
