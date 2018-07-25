import pytest
from frontera.core.components import States
from frontera.core.models import Request
from happybase import Connection
from frontera.contrib.backends.hbase import HBaseState, HBaseQueue
from frontera.contrib.backends.sqlalchemy import States as SQLAlchemyStates, Queue as SQLAlchemyQueue
from frontera.contrib.backends.sqlalchemy.models import StateModel, QueueModel
from frontera.contrib.backends.memory import MemoryStates, MemoryQueue
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


r1 = Request('https://www.example.com', meta={b'fingerprint': b'10',
             b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'}})
r2 = Request('http://example.com/some/page/', meta={b'fingerprint': b'11',
             b'domain': {b'name': b'example.com', b'fingerprint': b'82'}})
r3 = Request('http://www.scrapy.org', meta={b'fingerprint': b'12',
             b'domain': {b'name': b'www.scrapy.org', b'fingerprint': b'83'}})
r4 = r3.copy()


hbase_connection = None


def get_hbase_connection():
    global hbase_connection
    if hbase_connection is None:
        hbase_connection = Connection(host='hbase-docker', port=9090)
    return hbase_connection


@pytest.fixture(scope="module", params=["memory", "sqlalchemy", "hbase"])
def states(request):
    if request.param == "memory":
        ms = MemoryStates(100)
        yield ms
        return

    if request.param == "sqlalchemy":
        engine = create_engine('sqlite:///:memory:', echo=False)
        session_cls = sessionmaker()
        session_cls.configure(bind=engine)
        StateModel.__table__.create(bind=engine)
        sqla_states = SQLAlchemyStates(session_cls, StateModel, 100)
        yield sqla_states
        sqla_states.frontier_stop()
        engine.dispose()
        return

    if request.param == "hbase":
        conn = get_hbase_connection()
        states = HBaseState(conn, b'states', cache_size_limit=300000,
                            write_log_size=5000, drop_all_tables=True)
        yield states
        states.frontier_stop()
        return
    raise KeyError("Unknown backend param")


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


@pytest.fixture(scope="module", params=["memory", "sqlalchemy", "hbase"])
def queue(request):
    if request.param == "memory":
        mq = MemoryQueue(2)
        yield mq
        return

    if request.param == "sqlalchemy":
        engine = create_engine('sqlite:///:memory:', echo=False)
        session_cls = sessionmaker()
        session_cls.configure(bind=engine)
        QueueModel.__table__.create(bind=engine)
        sqla_queue = SQLAlchemyQueue(session_cls, QueueModel, 2)
        yield sqla_queue
        sqla_queue.frontier_stop()
        engine.dispose()
        return

    if request.param == "hbase":
        conn = get_hbase_connection()
        hq = HBaseQueue(conn, 2, b'queue')
        yield hq
        hq.frontier_stop()
        return
    raise KeyError("Unknown backend param")


def test_queue(queue):
    batch = [('10', 0.5, r1, True), ('11', 0.6, r2, True),
             ('12', 0.7, r3, True)]
    queue.schedule(batch)
    assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                                                       max_requests_per_host=10)]) == set([r3.url])
    assert set([r.url for r in queue.get_next_requests(10, 1, min_requests=3, min_hosts=1,
                                                       max_requests_per_host=10)]) == set([r1.url, r2.url])