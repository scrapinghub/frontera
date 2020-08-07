import pytest
from frontera.contrib.backends.sqlalchemy.components import States
from frontera.contrib.backends.sqlalchemy import Distributed
from frontera.core.models import Request
from sqlalchemy.orm import sessionmaker
from tests.mocks.frontier_manager import FakeFrontierManager


r1 = Request(
    'https://www.example.com',
    meta={
        b'fingerprint': b'10',
        b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'},
        b'state': 1
    }
)


@pytest.fixture(scope="module")
def sql_backend():
    manager = FakeFrontierManager.from_settings()
    backend = Distributed(manager)
    backend._init_strategy_worker(manager)
    yield backend
    backend.frontier_stop()
    backend.engine.dispose()


def test_check_and_create_tables_is_clear(sql_backend):
    session_cls = sessionmaker()
    session_cls.configure(bind=sql_backend.engine)
    another_session = session_cls()

    sql_backend.states.update_cache([r1])
    sql_backend.states.flush()
    model_states = sql_backend.models['StateModel']
    model_dm = sql_backend.models['DomainMetadataModel']
    assert another_session.query(model_states).count() == 1

    sql_backend.check_and_create_tables(False, True, (model_states, model_dm))
    assert another_session.query(model_states).count() == 0
