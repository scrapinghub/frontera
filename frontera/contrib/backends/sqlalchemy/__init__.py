from __future__ import absolute_import

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.reflection import Inspector

from frontera.contrib.backends import CommonBackend, CommonStorageBackend, CommonDistributedStorageBackend
from frontera.contrib.backends.sqlalchemy.components import Metadata, Queue, States
from frontera.contrib.backends.sqlalchemy.models import DeclarativeBase
from frontera.utils.misc import load_object


class SQLAlchemyBackend(CommonStorageBackend):

    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE')
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO')
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES')
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT')
        models = settings.get('SQLALCHEMYBACKEND_MODELS')

        self.engine = create_engine(engine, echo=engine_echo)
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        if drop_all_tables:
            DeclarativeBase.metadata.drop_all(self.engine)
        DeclarativeBase.metadata.create_all(self.engine)

        self.session_cls = sessionmaker()
        self.session_cls.configure(bind=self.engine)

        if clear_content:
            session = self.session_cls()
            for name, table in DeclarativeBase.metadata.tables.items():
                session.execute(table.delete())
            session.commit()
            session.close()
        self._metadata = Metadata(self.session_cls, self.models['MetadataModel'],
                                  settings.get('SQLALCHEMYBACKEND_CACHE_SIZE'))
        self._states = States(self.session_cls, self.models['StateModel'],
                              settings.get('STATE_CACHE_SIZE_LIMIT'))
        self._queue = self._create_queue(settings)

    def frontier_stop(self):
        super(SQLAlchemyBackend, self).frontier_stop()
        self.engine.dispose()

    def _create_queue(self, settings):
        return Queue(self.session, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))


class FIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy FIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created')


class LIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy LIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created_desc')


class DFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy DFS Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return -obj.meta[b'depth']


class BFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy BFS Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return obj.meta[b'depth']


BASE = CommonBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend


class Distributed(CommonDistributedStorageBackend):
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

    @classmethod
    def strategy_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES')
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT')
        model = b.models['StateModel']
        inspector = Inspector.from_engine(b.engine)

        if drop_all_tables:
            if model.__table__.name in inspector.get_table_names():
                model.__table__.drop(bind=b.engine)
        model.__table__.create(bind=b.engine)

        if clear_content:
            session = b.session_cls()
            session.execute(model.__table__.delete())
            session.close()
        b._states = States(b.session_cls, model,
                           settings.get('STATE_CACHE_SIZE_LIMIT'))
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES')
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT')
        inspector = Inspector.from_engine(b.engine)

        metadata_m = b.models['MetadataModel']
        queue_m = b.models['QueueModel']
        if drop:
            existing = inspector.get_table_names()
            if metadata_m.__table__.name in existing:
                metadata_m.__table__.drop(bind=b.engine)
            if queue_m.__table__.name in existing:
                queue_m.__table__.drop(bind=b.engine)
        metadata_m.__table__.create(bind=b.engine)
        queue_m.__table__.create(bind=b.engine)

        if clear_content:
            session = b.session_cls()
            session.execute(metadata_m.__table__.delete())
            session.execute(queue_m.__table__.delete())
            session.close()

        b._metadata = Metadata(b.session_cls, metadata_m,
                               settings.get('SQLALCHEMYBACKEND_CACHE_SIZE'))
        b._queue = Queue(b.session_cls, queue_m, settings.get('SPIDER_FEED_PARTITIONS'))
        return b
