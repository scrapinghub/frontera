from __future__ import absolute_import
import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
from sqlalchemy import Column, String, Integer, PickleType
from sqlalchemy import UniqueConstraint

from frontera import Backend
from frontera.utils.misc import load_object
from frontera.core.models import Request, Response

# Default settings
DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = True
DEFAULT_CLEAR_CONTENT = True
DEFAULT_MODELS = {
    'Page': 'frontera.contrib.backends.sqlalchemy.Page',
}

Base = declarative_base()


class DatetimeTimestamp(TypeDecorator):

    impl = String  # To use milliseconds in mysql
    timestamp_format = '%Y%m%d%H%M%S%f'

    def process_bind_param(self, value, _):
        if isinstance(value, datetime.datetime):
            return value.strftime(self.timestamp_format)
        raise ValueError('Not valid datetime')

    def process_result_value(self, value, _):
        return datetime.datetime.strptime(value, self.timestamp_format)


class Page(Base):
    __tablename__ = 'pages'
    __table_args__ = (
        UniqueConstraint('url'),
        {
            'mysql_charset': 'utf8',
            'mysql_engine': 'InnoDB',
            'mysql_row_format': 'DYNAMIC',
        },
    )

    class State:
        NOT_CRAWLED = 'NOT CRAWLED'
        QUEUED = 'QUEUED'
        CRAWLED = 'CRAWLED'
        ERROR = 'ERROR'

    url = Column(String(1024), nullable=False)
    fingerprint = Column(String(40), primary_key=True, nullable=False, index=True, unique=True)
    depth = Column(Integer, nullable=False)
    created_at = Column(DatetimeTimestamp(20), nullable=False)
    status_code = Column(String(20))
    state = Column(String(12))
    error = Column(String(20))
    meta = Column(PickleType())
    headers = Column(PickleType())
    cookies = Column(PickleType())
    method = Column(String(6))

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Page:%s>' % self.url


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

        # Create engine
        self.engine = create_engine(engine, echo=engine_echo)

        # Load models
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        # Drop tables if we have to
        if drop_all_tables:
            Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        # Create session
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

        # Clear content if we have to
        if clear_content:
            for name, table in Base.metadata.tables.items():
                self.session.execute(table.delete())

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    @property
    def page_model(self):
        return self.models['Page']

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.session.close()
        self.engine.dispose()

    def add_seeds(self, seeds):
        for seed in seeds:
            db_page, _ = self._get_or_create_db_page(seed)
        self.session.commit()

    def get_next_requests(self, max_next_requests, **kwargs):
        query = self.page_model.query(self.session)
        query = query.filter(self.page_model.state == Page.State.NOT_CRAWLED)
        query = self._get_order_by(query)
        if max_next_requests:
            query = query.limit(max_next_requests)
        next_pages = []
        for db_page in query:
            db_page.state = Page.State.QUEUED
            request = self.manager.request_model(url=db_page.url, meta=db_page.meta, headers=db_page.headers,
                                                 cookies=db_page.cookies, method=db_page.method)
            next_pages.append(request)
        self.session.commit()
        return next_pages

    def page_crawled(self, response, links):
        db_page, _ = self._get_or_create_db_page(response)
        db_page.state = Page.State.CRAWLED
        db_page.status_code = response.status_code
        for link in links:
            db_page_from_link, created = self._get_or_create_db_page(link)
            if created:
                db_page_from_link.depth = db_page.depth+1
        self.session.commit()

    def request_error(self, request, error):
        db_page, _ = self._get_or_create_db_page(request)
        db_page.state = Page.State.ERROR
        db_page.error = error
        self.session.commit()

    def _create_page(self, obj):
        db_page = self.page_model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.state = Page.State.NOT_CRAWLED
        db_page.url = obj.url
        db_page.created_at = datetime.datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = obj.request.method
            db_page.cookies = obj.request.cookies
        return db_page

    def _get_or_create_db_page(self, obj):
        if not self._request_exists(obj.meta['fingerprint']):
            db_page = self._create_page(obj)
            self.session.add(db_page)
            self.manager.logger.backend.debug('Creating request %s' % db_page)
            return db_page, True
        else:
            db_page = self.page_model.query(self.session).filter_by(fingerprint=obj.meta['fingerprint']).first()
            self.manager.logger.backend.debug('Request exists %s' % db_page)
            return db_page, False

    def _request_exists(self, fingerprint):
        q = self.page_model.query(self.session).filter_by(fingerprint=fingerprint)
        return self.session.query(q.exists()).scalar()

    def _get_order_by(self, query):
        raise NotImplementedError


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