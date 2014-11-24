from sqlalchemy import Column, String, Integer, Boolean, ForeignKey
from sqlalchemy.orm import relation
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import types

Base = declarative_base()


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


class BaseModel(object):
    __abstract__ = True

    @classmethod
    def get_pk_name(cls):
        return cls.__mapper__.primary_key[0].name

    @classmethod
    def get_pk_field(cls):
        return getattr(cls, cls.get_pk_name())

    @classmethod
    def query(cls, session):
        return session.query(cls)

    @classmethod
    def query_pk(cls, session):
        return session.query(cls.get_pk_field())

    @classmethod
    def get_or_create(cls, session, **kwargs):
        instance = session.query(cls).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            instance = cls(**kwargs)
            session.add(instance)
            return instance, True

    def get_pk(self):
        return getattr(self, self.get_pk_name())

    def exists(self, session):
        q = self.query(session).filter_by(**{self.get_pk_name(): self.get_pk()})
        return session.query(q.exists()).scalar()


class Model(Base, BaseModel):
    pass


class CrawlPageRelation(Model):
    __tablename__ = 'crawl_page_relations'
    parent_id = Column(Integer, ForeignKey('crawl_pages.id'), primary_key=True, index=True)
    child_id = Column(Integer, ForeignKey('crawl_pages.id'), primary_key=True, index=True)


class CrawlPage(Model):
    __tablename__ = 'crawl_pages'
    __table_args__ = (
        UniqueConstraint('url'),
    )

    id = Column(Integer, primary_key=True, nullable=False, index=True, unique=True)
    url = Column(String(1000))
    status = Column(String(50))
    n_redirects = Column(Integer, default=0)
    is_seed = Column(Boolean, default=False)
    referers = relation(
        'CrawlPage',
        secondary='crawl_page_relations',
        primaryjoin=CrawlPageRelation.child_id == id,
        secondaryjoin=CrawlPageRelation.parent_id == id,
        backref="links")

    def __repr__(self):
        return '<%s:%s%s>' % (self.id, self.url, '*' if self.is_seed else '')

    def _get_status_code(self):
        try:
            return int(self.status)
        except TypeError:
            return None

    @property
    def has_errors(self):
        return self._get_status_code() is None

    @property
    def is_redirection(self):
        status_code = self._get_status_code()
        if status_code:
            return 300 <= status_code < 400
        else:
            return False
