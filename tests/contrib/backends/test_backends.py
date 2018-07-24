import pytest
from frontera.core.components import States
from frontera.core.models import Request
from happybase import Connection
from frontera.contrib.backends.hbase import HBaseState
from frontera.contrib.backends.sqlalchemy import States as SQLAlchemyStates
from frontera.contrib.backends.sqlalchemy.models import StateModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


r1 = Request('https://www.example.com', meta={b'fingerprint': b'10',
             b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'}})
r2 = Request('http://example.com/some/page/', meta={b'fingerprint': b'11',
             b'domain': {b'name': b'example.com', b'fingerprint': b'82'}})
r3 = Request('http://www.scrapy.org', meta={b'fingerprint': b'12',
             b'domain': {b'name': b'www.scrapy.org', b'fingerprint': b'83'}})
r4 = r3.copy()


@pytest.fixture
def hbase_states():
    connection = Connection(host='hbase-docker', port=9090)
    states = HBaseState(connection, b'states', cache_size_limit=300000,
                        write_log_size=5000, drop_all_tables=True)
    return states


def sqlalchemy_states():
    engine = create_engine('sqlite:///:memory:', echo=False)
    session_cls = sessionmaker()
    session_cls.configure(bind=engine)
    StateModel.__table__.create(bind=engine)
    return SQLAlchemyStates(session_cls, StateModel, 100)


@pytest.mark.parametrize("states", [hbase_states(), sqlalchemy_states()])
def test_states(states):
    states.set_states([r1, r2, r3])
    assert [r.meta[b'state'] for r in [r1, r2, r3]] == [States.NOT_CRAWLED]*3
    states.update_cache([r1, r2, r3])
    states.flush()

    r1.meta[b'state'] = States.CRAWLED
    r2.meta[b'state'] = States.ERROR
    r3.meta[b'state'] = States.QUEUED
    states.update_cache([r1, r2, r3])
    states.flush()

    r1.meta[b'state'] = States.NOT_CRAWLED
    r2.meta[b'state'] = States.NOT_CRAWLED
    r3.meta[b'state'] = States.NOT_CRAWLED

    states.fetch([b'83'])
    states.set_states([r1, r2, r4])
    assert r4.meta[b'state'] == States.QUEUED
    assert r1.meta[b'state'] == States.CRAWLED
    assert r2.meta[b'state'] == States.ERROR