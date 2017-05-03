from __future__ import absolute_import

import six

from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker

from frontera.core.components import DistributedBackend
from frontera.utils.misc import load_object

from .metadata import Metadata
from .queue import Queue
from .states import States


class Distributed(DistributedBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        engine = settings.get('SQLALCHEMYBACKEND_ENGINE')
        engine_echo = settings.get('SQLALCHEMYBACKEND_ENGINE_ECHO')
        models = settings.get('SQLALCHEMYBACKEND_MODELS')
        self.engine = create_engine(engine, echo=engine_echo)
        self.models = {
            name: load_object(klass)
            for name, klass in six.iteritems(models)
        }
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

        b._metadata = Metadata(
            b.session_cls,
            metadata_m,
            settings.get('SQLALCHEMYBACKEND_CACHE_SIZE'),
        )
        b._queue = Queue(
            b.session_cls,
            queue_m,
            settings.get('SPIDER_FEED_PARTITIONS'),
        )

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

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states]:
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
