# -*- coding: utf-8 -*-

from __future__ import absolute_import
from frontera.contrib.backends.remote.codecs.json import Encoder as JsonEncoder, Decoder as JsonDecoder
from frontera.contrib.backends.remote.codecs.msgpack import Encoder as MsgPackEncoder, Decoder as MsgPackDecoder
from frontera.core.models import Request, Response
import pytest


@pytest.mark.parametrize(
    ('encoder', 'decoder'), [
        (MsgPackEncoder, MsgPackDecoder),
        (JsonEncoder, JsonDecoder)
    ]
)
def test_codec(encoder, decoder):
    def check_request(req1, req2):
        assert req1.url == req2.url and req1.meta == req2.meta and req1.headers == req2.headers \
            and req1.method == req2.method

    enc = encoder(Request, send_body=True)
    dec = decoder(Request, Response)
    req = Request(url="http://www.yandex.ru",method=b'GET', meta={b"test": b"shmest"}, headers={b'reqhdr': b'value'})
    req2 = Request(url="http://www.yandex.ru/search")
    msgs = [
        enc.encode_add_seeds([req]),
        enc.encode_page_crawled(Response(url="http://www.yandex.ru", body=b'SOME CONTENT', headers={b'hdr': b'value'},
                                         request=req))
        enc.encode_links_extracted(req, [req2])
        enc.encode_request_error(req, "Host not found"),
        enc.encode_update_score(req, 0.51, True),
        enc.encode_new_job_id(1),
        enc.encode_offset(0, 28796),
        enc.encode_request(req)
    ]

    it = iter(msgs)

    o = dec.decode(next(it))
    assert o[0] == 'add_seeds'
    assert type(o[1]) == list
    req_d = o[1][0]
    check_request(req_d, req)
    assert type(req_d) == Request

    o = dec.decode(next(it))
    assert o[0] == 'page_crawled'
    assert type(o[1]) == Response
    assert o[1].url == req.url and o[1].body == b'SOME CONTENT' and o[1].meta == req.meta

    o = dec.decode(it.next())
    assert o[0] == 'links_extracted'
    assert type(o[1]) == Request
    assert o[1].url == req.url and o[1].meta == req.meta
    assert type(o[2]) == list
    req_d = o[2][0]
    assert type(req_d) == Request
    assert req_d.url == req2.url

    o_type, o_req, o_error = dec.decode(next(it))
    assert o_type == 'request_error'
    check_request(o_req, req)
    assert o_error == "Host not found"

    o_type, o_req2, score, schedule = dec.decode(next(it))
    assert o_type == 'update_score'
    assert o_req2.url == req.url and o_req2.meta == req.meta and o_req2.headers == req.headers
    assert score == 0.51
    assert schedule is True

    o_type, job_id = dec.decode(next(it))
    assert o_type == 'new_job_id'
    assert job_id == 1

    o_type, partition_id, offset = dec.decode(next(it))
    assert o_type == 'offset'
    assert partition_id == 0
    assert offset == 28796

    o = dec.decode_request(next(it))
    check_request(o, req)
