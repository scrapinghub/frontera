from __future__ import absolute_import

from frontera.contrib.backends.sqlalchemy.components import Metadata, Queue, States, DomainMetadata
from frontera.contrib.backends.sqlalchemy.models import DeclarativeBase
from frontera.core.components import DistributedBackend
from frontera.utils.misc import load_object
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker


class Distributed(DistributedBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE')
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO')
        models = settings.get('SQLALCHEMYBACKEND_MODELS')
        self.engine = create_engine(engine, echo=engine_echo)
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])
        self.session_cls = sessionmaker()
        self.session_cls.configure(bind=self.engine)
        self._metadata = None
        self._queue = None
        self._states = None
        self._domain_metadata = None

    def check_and_create_tables(self, is_drop, is_clear, models):
        inspector = Inspector.from_engine(self.engine)
        for model in models:
            if is_drop:
                if model.__table__.name in inspector.get_table_names():
                    model.__table__.drop(bind=self.engine)
            if model.__table__.name not in inspector.get_table_names():
                model.__table__.create(bind=self.engine)
            if is_clear:
                session = self.session_cls()
                session.execute(model.__table__.delete())
                session.close()

    def _init_strategy_worker(self, manager):
        settings = manager.settings
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES')
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT')
        model_states = self.models['StateModel']
        model_dm = self.models['DomainMetadataModel']
        self.check_and_create_tables(drop_all_tables, clear_content, (model_states, model_dm))
        self._states = States(self.session_cls, model_states,
                              settings.get('STATE_CACHE_SIZE_LIMIT'))
        self._domain_metadata = DomainMetadata(self.session_cls)

    def _init_db_worker(self, manager):
        settings = manager.settings
        drop = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES')
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT')
        metadata_m = self.models['MetadataModel']
        queue_m = self.models['QueueModel']
        self.check_and_create_tables(drop, clear_content, (metadata_m, queue_m))
        self._metadata = Metadata(self.session_cls, metadata_m,
                                  settings.get('SQLALCHEMYBACKEND_CACHE_SIZE'))
        self._queue = Queue(self.session_cls, queue_m, settings.get('SPIDER_FEED_PARTITIONS'))

    @classmethod
    def strategy_worker(cls, manager):
        b = cls(manager)
        b._init_strategy_worker(manager)
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        b._init_db_worker(manager)
        return b

    @classmethod
    def local(cls, manager):
        b = cls(manager)
        b._init_db_worker(manager)
        b._init_strategy_worker(manager)
        return b

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states

    @property
    def domain_metadata(self):
        return self._domain_metadata

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component:
                component.frontier_stop()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        return batch

    def page_crawled(self, response):
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        self.metadata.links_extracted(request, links)

    def request_error(self, request, error):
        self.metadata.request_error(request, error)

    def finished(self):
        raise NotImplementedError

