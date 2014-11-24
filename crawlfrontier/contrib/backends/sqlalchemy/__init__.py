import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from crawlfrontier import Backend
from crawlfrontier.utils.misc import load_object

Base = declarative_base()

# Default settings
DEFAULT_ENGINE = 'sqlite:///:memory:'
DEFAULT_ENGINE_ECHO = False
DEFAULT_DROP_ALL_TABLES = True
DEFAULT_CLEAR_CONTENT = True
DEFAULT_MODELS = {
    'Page': 'crawlfrontier.contrib.backends.sqlalchemy.models.Page',
}


class SQLiteBackend(Backend):
    name = 'SQLite Backend'

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

    # ---------------------------- FRONTIER METHODS --------------------------------

    def add_seeds(self, links):
        # Log
        self.manager.logger.backend.debug('ADD_SEEDS n_links=%s' % len(links))

        pages = []
        for link in links:
            # Get or create page from link
            db_page, created = self._get_or_create_db_page_from_link(link=link, now=datetime.datetime.utcnow())

            # Create frontier page
            page = self._get_frontier_page_from_db_page(db_page)
            pages.append(page)

        # Commit and return pages
        self.session.commit()
        return pages

    def page_crawled(self, page, links):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED page=%s status=%s links=%s' %
                                          (page, page.status, len(links)))

        # Get or create page
        db_page, created = self._page_crawled(page)

        # Update crawled fields
        db_page.n_crawls += 1
        db_page.state = self.manager.page_model.State.CRAWLED

        # Create links
        for link in links:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            db_link, db_link_created = self._get_or_create_db_page_from_link(link, datetime.datetime.utcnow())
            if db_link_created:
                db_link.depth = db_page.depth+1

        # Create frontier page
        page = self._get_frontier_page_from_db_page(db_page)

        # Commit and return page
        self.session.commit()
        return page

    def page_crawled_error(self, page, error):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED_ERROR page=%s error=%s' % (page, error))

        # process page crawled
        db_page, created = self._page_crawled(page)

        # Update error fields
        db_page.n_errors += 1
        db_page.state = self.manager.page_model.State.ERROR

        # Create frontier page
        page = self._get_frontier_page_from_db_page(db_page)

        # Commit and return page
        self.session.commit()
        return page

    def get_next_pages(self, max_next_pages):
        # Log
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)

        # query pages
        query = self.page_model.query(self.session)
        query = query.filter(self.page_model.state == self.page_model.States.NOT_CRAWLED)
        query = self._get_order_by(query)
        if max_next_pages:
            query = query.limit(max_next_pages)

        # update page fields and create frontier pages to return
        next_pages = []
        for db_page in query:
            db_page.state = self.page_model.States.QUEUED
            db_page.last_update = datetime.datetime.utcnow()
            next_pages.append(self._get_frontier_page_from_db_page(db_page))

        # Commit and return pages
        self.session.commit()
        return next_pages

    def get_page(self, link):
        db_page = self.page_model.query(self.session).filter_by(fingerprint=link.fingerprint).first()
        if db_page:
            return self._get_frontier_page_from_db_page(db_page)
        else:
            return None

    # ---------------------------- DB METHODS --------------------------------

    def _get_or_create_db_page_from_link(self, link, now):
        db_page, created = self._get_or_create_db_page(fingerprint=link.fingerprint, url=link.url, now=now)
        if created:
            self.manager.logger.backend.debug('Creating page %s from link %s' % (db_page, link))
        else:
            self.manager.logger.backend.debug('Page %s exists' % db_page)
        return db_page, created

    def _get_or_create_db_page(self, fingerprint, url, now):
        exists = self._db_page_exists(fingerprint)
        if not exists:
            db_page = self.page_model()
            db_page.fingerprint = fingerprint
            db_page.state = db_page.States.NOT_CRAWLED
            db_page.url = url
            db_page.depth = 0
            db_page.created_at = now
            self.session.add(db_page)
        else:
            db_page = self.page_model.query(self.session).filter_by(fingerprint=fingerprint).first()
        db_page.last_update = now
        db_page.last_iteration = self.manager.iteration
        return db_page, not exists

    def _db_page_exists(self, fingerprint):
        q = self.page_model.query(self.session).filter_by(fingerprint=fingerprint)
        return self.session.query(q.exists()).scalar()

    def _page_crawled(self, page):
        # Get timestamp
        now = datetime.datetime.utcnow()

        # Get or create page from frontier page
        db_page, created = self._get_or_create_db_page(fingerprint=page.fingerprint, url=page.url, now=now)

        # Update fields
        db_page.last_update = now
        db_page.status = page.status

        return db_page, created

    # ---------------------------- FRONTIER OBJECT METHODS --------------------------------

    def _get_frontier_page_from_db_page(self, db_page):
        frontier_page = self.manager.page_model(url=db_page.url)
        frontier_page.state = db_page.state
        frontier_page.depth = db_page.depth
        frontier_page.created_at = db_page.created_at
        frontier_page.last_update = db_page.last_update
        frontier_page.status = db_page.status
        frontier_page.n_adds = db_page.n_adds
        frontier_page.n_queued = db_page.n_queued
        frontier_page.n_crawls = db_page.n_crawls
        frontier_page.n_errors = db_page.n_errors
        return frontier_page

    # ---------------------------- ORDER METHODS --------------------------------

    def _get_order_by(self, query):
        raise NotImplementedError


class FIFOBackend(SQLiteBackend):
    name = 'SQLite Backend FIFO'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at)


class LIFOBackend(SQLiteBackend):
    name = 'SQLite Backend LIFO'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.created_at.desc())


class DFSBackend(SQLiteBackend):
    name = 'SQLite Backend DFS'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth.desc(), self.page_model.created_at)


class BFSBackend(SQLiteBackend):
    name = 'SQLite Backend BFS'

    def _get_order_by(self, query):
        return query.order_by(self.page_model.depth, self.page_model.created_at)


BASE = SQLiteBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend