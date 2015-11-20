from __future__ import absolute_import

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from frontera import Backend
from frontera.utils.misc import load_object
from frontera.contrib.backends.sqlalchemy.models import DeclarativeBase
from frontera.contrib.backends.sqlalchemy.components import SQLAlchemyMetadata, SQLAlchemyQueue, SQLAlchemyState


# Default settings
DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = True
DEFAULT_CLEAR_CONTENT = True
DEFAULT_MODELS = {
    'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
    'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
    'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel'
}


class SQLAlchemyBackend(Backend):
    component_name = 'SQLAlchemy Backend'

    def __init__(self, manager):
        self.manager = manager

        # Get settings
        settings = manager.settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE', DEFAULT_ENGINE)
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO', DEFAULT_ENGINE_ECHO)
        drop_all_tables = settings.get('SQLALCHEMYBACKEND_DROP_ALL_TABLES', DEFAULT_DROP_ALL_TABLES)
        clear_content = settings.get('SQLALCHEMYBACKEND_CLEAR_CONTENT', DEFAULT_CLEAR_CONTENT)
        models = settings.get('SQLALCHEMYBACKEND_MODELS', DEFAULT_MODELS)

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
            session.close()
        self.metadata = SQLAlchemyMetadata(self.session_cls, self.models['MetadataModel'])
        self.queue = SQLAlchemyQueue(self.session_cls, self.models['QueueModel'], self.models['MetadataModel'], 1)
        self.states = SQLAlchemyState(self.session_cls, self.models['StateModel'],
                                      settings.get('STATE_CACHE_SIZE_LIMIT'))

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        self.metadata.frontier_start()
        self.queue.frontier_start()
        self.states.frontier_start()
        self.queue_size = self.queue.count()

    def frontier_stop(self):
        self.metadata.frontier_stop()
        self.queue.frontier_stop()
        self.states.frontier_stop()
        self.engine.dispose()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)
        self.states.fetch([seed.meta['fingerprint'] for seed in seeds])
        self.states.set_states(seeds)
        self._schedule(seeds)

    def _schedule(self, requests):
        batch = {}
        queue_incr = 0
        for request in requests:
            schedule = True if request.meta['state'] in [SQLAlchemyState.NOT_CRAWLED, SQLAlchemyState.ERROR, None] else False
            batch[request.meta['fingerprint']] = (1.0, request.url, schedule)

            if schedule:
                queue_incr += 1
        self.queue.schedule(batch)
        self.queue_size += queue_incr

    def get_next_requests(self, max_next_requests, **kwargs):
        batch = self.queue.get_next_requests(max_next_requests, 0, **kwargs)
        self.queue_size -= len(batch)
        return batch

    def page_crawled(self, response, links):
        response.meta['state'] = SQLAlchemyState.CRAWLED
        self.metadata.page_crawled(response, links)
        self.states.update_cache(response)
        self.states.fetch([link.meta['fingerprint'] for link in links])
        self.states.set_states(links)
        self._schedule(links)

    def request_error(self, request, error):
        request.meta['state'] = SQLAlchemyState.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def finished(self):
        return self.queue_size == 0


class FIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy FIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at)


class LIFOBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy LIFO Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at.desc())


class DFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy DFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth.desc(), self.page_model.created_at)


class BFSBackend(SQLAlchemyBackend):
    component_name = 'SQLAlchemy BFS Backend'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth, self.page_model.created_at)


BASE = SQLAlchemyBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend