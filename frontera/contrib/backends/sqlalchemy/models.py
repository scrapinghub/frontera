from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    PickleType,
    SmallInteger,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()


class MetadataModel(DeclarativeBase):
    __tablename__ = "metadata"
    __table_args__ = (
        {
            "mysql_charset": "utf8",
            "mysql_engine": "InnoDB",
            "mysql_row_format": "DYNAMIC",
        },
    )

    fingerprint = Column(String(40), primary_key=True, nullable=False)
    url = Column(String(1024), nullable=False)
    depth = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    fetched_at = Column(DateTime, nullable=True)
    status_code = Column(String(20))
    score = Column(Float)
    error = Column(String(128))
    meta = Column(PickleType())
    headers = Column(PickleType())
    cookies = Column(PickleType())
    method = Column(String(6))

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return f"<Metadata:{self.url} ({self.fingerprint})>"


class StateModel(DeclarativeBase):
    __tablename__ = "states"
    __table_args__ = (
        {
            "mysql_charset": "utf8",
            "mysql_engine": "InnoDB",
            "mysql_row_format": "DYNAMIC",
        },
    )

    fingerprint = Column(String(40), primary_key=True, nullable=False)
    state = Column(SmallInteger())

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return f"<State:{self.fingerprint}={self.state}>"


class QueueModelMixin:
    __table_args__ = (
        {
            "mysql_charset": "utf8",
            "mysql_engine": "InnoDB",
            "mysql_row_format": "DYNAMIC",
        },
    )

    id = Column(Integer, primary_key=True)
    partition_id = Column(Integer, index=True)
    score = Column(Float, index=True)
    url = Column(String(1024), nullable=False)
    fingerprint = Column(String(40), nullable=False)
    host_crc32 = Column(Integer, nullable=False)
    meta = Column(PickleType())
    headers = Column(PickleType())
    cookies = Column(PickleType())
    method = Column(String(6))
    created_at = Column(BigInteger, index=True)
    depth = Column(SmallInteger)


class QueueModel(QueueModelMixin, DeclarativeBase):
    __tablename__ = "queue"

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return f"<Queue:{self.url} ({self.id})>"
