from sqlalchemy import Column, String, Integer, TIMESTAMP
from sqlalchemy import types
from sqlalchemy import UniqueConstraint

from crawlfrontier.contrib.backends.sqlalchemy import Base
from crawlfrontier.core.models import Page as FrontierPage

FINGERPRINT = String(40)
URL_FIELD = String(1000)


class Choice(types.TypeDecorator):
    impl = types.CHAR

    def __init__(self, choices, default, **kwargs):
        self.choices = dict(choices)
        values = [k for k, v in self.choices.iteritems()]
        if default not in values:
            raise ValueError("default value '%s' not found in choices %s" % (default, values))
        self.default = default
        super(Choice, self).__init__(**kwargs)

    def process_bind_param(self, value, dialect):
        return value or self.default

    def process_result_value(self, value, dialect):
        return self.choices[value]


class Model(Base):
    __abstract__ = True

    @classmethod
    def query(cls, session):
        return session.query(cls)


class PageBase(Model):
    __abstract__ = True
    __tablename__ = 'pages'
    __table_args__ = (
        UniqueConstraint('url'),
    )

    class States:
        NOT_CRAWLED = 'N'
        QUEUED = 'Q'
        CRAWLED = 'C'
        ERROR = 'E'

    STATES = [
        (States.NOT_CRAWLED, FrontierPage.State.NOT_CRAWLED),
        (States.QUEUED, FrontierPage.State.QUEUED),
        (States.CRAWLED, FrontierPage.State.CRAWLED),
        (States.ERROR, FrontierPage.State.ERROR),
    ]

    fingerprint = Column(FINGERPRINT, primary_key=True, nullable=False, index=True, unique=True)
    url = Column(URL_FIELD, nullable=False)
    depth = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)
    last_update = Column(TIMESTAMP, nullable=False)
    status = Column(String(20))
    last_redirects = Column(Integer)
    last_iteration = Column(Integer, nullable=False)
    state = Column(Choice(choices=STATES, default=States.NOT_CRAWLED))
    n_adds = Column(Integer, default=0)
    n_queued = Column(Integer, default=0)
    n_crawls = Column(Integer, default=0)
    n_errors = Column(Integer, default=0)

    def __repr__(self):
        return '<Page:%s>' % self.url

    @property
    def is_seed(self):
        return self.depth == 0


class Page(PageBase):
    __table_args__ = {'extend_existing': True}
