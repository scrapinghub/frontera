from __future__ import absolute_import

from collections import OrderedDict

from pytest import fixture, raises

from frontera.contrib.backends.remote.messagebus import (
    MessageBusBackend,
    aggregate_per_host,
    get_host_fprint,
)
from frontera.core.models import Request, Response
from frontera.settings import Settings

import tests


patch = tests.mock.patch
Mock = tests.mock.Mock


@fixture
def urls():
    return [
        'http://www.example.com/blog',
        'http://www.scrapy.org/',
        'http://www.test.com/some/page',
    ]


@fixture
def request(requests):
    return Request(
        'https://www.example.com/',
        meta={
            b'domain': {
                b'fingerprint': b'0caaf24ab1a0c33440c06afe99df986365b0781f',
            },
        },
    )


@fixture
def requests(urls):
    return [
        Request(
            url,
            meta={
                b'domain': {
                    b'fingerprint': (u'%d' % i).encode('ascii'),
                },
            },
        )
        for i, url in enumerate(urls, 1)
    ]


def test_requests(requests):
    assert requests[0].meta[b'domain'][b'fingerprint'] == b'1'
    assert requests[1].meta[b'domain'][b'fingerprint'] == b'2'
    assert requests[2].meta[b'domain'][b'fingerprint'] == b'3'


@fixture
def response(request):
    return Response(request.url, body='body', request=request)


def test_response(response, request):
    assert response.url == 'https://www.example.com/'
    assert response.request is request


@fixture
def settings():
    return Settings(attributes={
        'MESSAGE_BUS': 'tests.mocks.message_bus.FakeMessageBus',
        'STORE_CONTENT': True,
        'MESSAGE_BUS_USE_OVERUSED_BUFFER': False,
        'MESSAGE_BUS_REQUEST_AGGREGATION_FUNCTION': (
            'frontera.contrib.backends.remote.messagebus.aggregate_per_host'
        ),
    })


@fixture
def manager(settings):
    manager = type('manager', (object,), {})

    manager.settings = settings
    manager.request_model = Request
    manager.response_model = Response

    return manager


@fixture
def mbb(manager):
    return MessageBusBackend(manager)


@fixture
def mbb_buffered(manager):
    manager.settings.MESSAGE_BUS_USE_OVERUSED_BUFFER = True

    return MessageBusBackend(manager)


def test_aggregate_per_host(requests):
    assert aggregate_per_host(requests) == {
        b'1': [requests[0]],
        b'2': [requests[1]],
        b'3': [requests[2]],
    }


def test_aggregate_per_host_no_host_key(requests):
    del requests[0].meta[b'domain'][b'fingerprint']
    del requests[2].meta[b'domain']

    assert aggregate_per_host(requests) == {
        b'2': [requests[1]],
    }


def test_get_host_fprint(request):
    host_fprint = request.meta[b'domain'][b'fingerprint']

    assert get_host_fprint(request) is host_fprint


def test_get_host_fprint_no_domain(request):
    del request.meta[b'domain']

    assert get_host_fprint(request) is None


def test_get_host_fprint_no_fp(request):
    del request.meta[b'domain'][b'fingerprint']

    assert get_host_fprint(request) is None


def test_partition_id_eq_n(manager):
    manager.settings.SPIDER_FEED_PARTITIONS = 1
    manager.settings.SPIDER_PARTITION_ID = 1

    with raises(ValueError):
        MessageBusBackend(manager)


def test_partition_id_gt_n(manager):
    manager.settings.SPIDER_FEED_PARTITIONS = 1
    manager.settings.SPIDER_PARTITION_ID = 2

    with raises(ValueError):
        MessageBusBackend(manager)


def test_partition_id_lt_0(manager):
    manager.settings.SPIDER_PARTITION_ID = -1

    with raises(ValueError):
        MessageBusBackend(manager)


def test_from_manager(manager):
    MessageBusBackend.from_manager(manager)


def test_frontier_start(mbb):
    mbb.frontier_start()


def test_frontier_stop(mbb):
    mbb.spider_log_producer = Mock()

    mbb.frontier_stop()

    assert mbb.spider_log_producer.flush.called


def test_mbb_add_seeds(mbb, requests, urls):
    mbb._aggregate = Mock(return_value=OrderedDict((
        (b'x', [requests[0]]),
        (b'y', [requests[1], requests[2]]),
    )))

    mbb.add_seeds(requests)

    seed_groups = [
        requests
        for _, requests in [
            mbb._decoder.decode(m)
            for m in mbb.spider_log_producer.messages
        ]
    ]

    assert len(seed_groups) == 2
    assert len(seed_groups[0]) == 1
    assert len(seed_groups[1]) == 2
    assert set(seed.url for seed in seed_groups[0]) == {urls[0]}
    assert set(seed.url for seed in seed_groups[1]) == {urls[1], urls[2]}


def test_page_crawled(mbb, response):
    mbb.page_crawled(response)

    _, page = mbb._decoder.decode(mbb.spider_log_producer.messages[0])

    assert len(mbb.spider_log_producer.messages) == 1
    assert page.request.url == response.request.url
    assert page.body == response.body


def test_mbb_links_extracted(mbb, request, requests, urls):
    mbb._aggregate = Mock(return_value=OrderedDict((
        (b'x', [requests[0]]),
        (b'y', [requests[1], requests[2]]),
    )))

    mbb.links_extracted(request, requests)

    link_groups = [
        (request, links)
        for _, request, links in [
            mbb._decoder.decode(m)
            for m in mbb.spider_log_producer.messages
        ]
    ]

    assert len(link_groups) == 2
    assert set(_request.url for _request, _ in link_groups) == {request.url}
    assert set(link.url for link in link_groups[0][1]) == {urls[0]}
    assert set(link.url for link in link_groups[1][1]) == {urls[1], urls[2]}


def test_request_error(mbb, request):
    mbb.request_error(request, 'error')

    _, error_request, error_message = mbb._decoder.decode(
        mbb.spider_log_producer.messages[0]
    )

    assert len(mbb.spider_log_producer.messages) == 1
    assert error_request.url == request.url
    assert error_message == 'error'


def test_mbb_get_next_requests(mbb, requests, urls):
    encoded_requests = list(map(mbb._encoder.encode_request, requests))

    mbb.consumer.put_messages(encoded_requests)
    mbb.consumer._set_offset(0)

    next_requests = set(
        request.url
        for request in mbb.get_next_requests(10)
    )
    _, partition_id, offset = mbb._decoder.decode(
        mbb.spider_log_producer.messages[0]
    )

    assert len(mbb.spider_log_producer.messages) == 1
    assert next_requests == set(urls)
    assert partition_id == 0
    assert offset == 0

    next_requests = set(
        request.url
        for request in mbb.get_next_requests(10)
    )

    assert not next_requests


def test_mbb_get_next_requests_garbage(mbb, requests, urls):
    mbb.consumer.put_messages([b'\0'])
    mbb.consumer._set_offset(0)

    next_requests = set(
        request.url
        for request in mbb.get_next_requests(10)
    )

    _, partition_id, offset = mbb._decoder.decode(
        mbb.spider_log_producer.messages[0]
    )

    assert len(mbb.spider_log_producer.messages) == 1
    assert not next_requests
    assert partition_id == 0
    assert offset == 0


def test_mbb_get_next_requests_buffered(mbb_buffered, requests, urls):
    encode_request = mbb_buffered._encoder.encode_request
    encoded_requests = list(map(encode_request, requests))

    mbb_buffered.consumer.put_messages(encoded_requests)
    mbb_buffered.consumer._set_offset(0)

    next_requests = set(
        request.url
        for request in mbb_buffered.get_next_requests(
            max_n_requests=10,
            overused_keys=['www.example.com'],
            key_type='domain',
        )
    )

    assert next_requests == set(urls[1:])


def test_finished(mbb):
    assert mbb.finished() is False


def test_metadata(mbb):
    assert mbb.metadata is None


def test_queue(mbb):
    assert mbb.queue is None


def test_states(mbb):
    assert mbb.states is None
