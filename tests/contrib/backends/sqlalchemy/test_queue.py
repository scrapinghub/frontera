from unittest.mock import Mock, patch
from frontera.core.models import Request
import pytest
from frontera.contrib.backends.sqlalchemy.components import Queue
from frontera.contrib.backends.sqlalchemy.models import QueueModel


@pytest.fixture(scope='function')
def http_request():
    return Request('http://www.hello.com', meta={b'fingerprint': '123', b'depth': 1, b'score': 0.5})


@pytest.fixture(scope='function')
def queue_component(request):
    order_by = request.param if request.param else 'default'
    return Queue(Mock(), QueueModel, 1, order_by)


class TestSqlAlchemyQueue:
    @pytest.mark.parametrize('queue_component', ['default'], indirect=['queue_component'])
    def test_frontier_stop(self, queue_component):
        with patch.object(queue_component.session, 'close') as session_close:
            queue_component.frontier_stop()
            session_close.assert_called_once()

    @pytest.mark.parametrize('queue_component', ['default'], indirect=['queue_component'])
    def test_order_by_default(self, queue_component):
        with patch.object(queue_component.session, 'query') as query:
            with patch.object(query, 'order_by') as order_by:
                with patch.object(queue_component.queue_model.score, 'desc') as desc:
                    queue_component._order_by(query)
                    order_by.assert_called_once()
                    # should order score by descending order
                    desc.assert_called_once()

    @pytest.mark.parametrize('queue_component', ['created'], indirect=['queue_component'])
    def test_order_by_created(self, queue_component):
        with patch.object(queue_component.session, 'query') as query:
            with patch.object(query, 'order_by') as order_by:
                queue_component._order_by(query)
                assert order_by.called

    @pytest.mark.parametrize('queue_component', ['created_desc'], indirect=['queue_component'])
    def test_order_by_created_desc(self, queue_component):
        with patch.object(queue_component.session, 'query') as query:
            with patch.object(query, 'order_by') as order_by:
                with patch.object(queue_component.queue_model.created_at, 'desc') as desc:
                    queue_component._order_by(query)
                    order_by.assert_called_once()
                    # should order created_at by descending order
                    desc.assert_called_once()

    @pytest.mark.parametrize('queue_component', [('default')], indirect=['queue_component'])
    def test_get_next_requests(self, queue_component):
        with patch.object(queue_component.session, 'commit') as session_commit:
            with patch.object(queue_component, '_order_by') as _order_by:
                with patch.object(queue_component.session, 'delete') as session_delete:
                    item = Mock(url='http://www.hello.com', method='GET', fingerprint='123',
                        meta = {}, headers={}, cookies={})
                    _order_by.return_value.limit.return_value = [item]
                    queue_component.get_next_requests(2, 0)
                    session_delete.assert_called_once() # should delete item from the queue
                    session_commit.assert_called_once()

    @pytest.mark.parametrize('queue_component', [('default')], indirect=['queue_component'])
    def test_get_next_requests_exception(self, queue_component):
        with patch.object(queue_component, '_order_by') as _order_by:
            with patch.object(queue_component.session, 'rollback') as session_rollback:
                _order_by.return_value = {}
                queue_component.get_next_requests(2, 0)
                session_rollback.assert_called_once()

    @pytest.mark.parametrize('queue_component', [('default')], indirect=['queue_component'])
    def test_schedule(self, queue_component, http_request):
        with patch.object(queue_component.session, 'commit') as session_commit:
            with patch.object(queue_component.session, 'bulk_save_objects') as bulk_save:
                queue_component.schedule([['123', Mock(), http_request, True]])
                bulk_save.assert_called_once()
                session_commit.assert_called_once()

    @pytest.mark.parametrize('queue_component', [('default')], indirect=['queue_component'])
    def test_schedule_no_hostname(self, queue_component, http_request):
        with patch.object(queue_component.session, 'commit') as session_commit:
            with patch.object(queue_component.session, 'bulk_save_objects') as bulk_save:
                queue_component.schedule([['123', Mock(), Request('hello', meta={b'depth': 1}), True]])
                bulk_save.assert_called_once()
                session_commit.assert_called_once()

    @pytest.mark.parametrize('queue_component', [('default')], indirect=['queue_component'])
    def test_count(self, queue_component):
        with patch.object(queue_component.session, 'query') as session_query:
            session_query.return_value.count.return_value = 2
            value = queue_component.count()
            assert value is 2
