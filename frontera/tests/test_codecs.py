# -*- coding: utf-8 -*-

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
        assert req1.url == req2.url and req1.meta == req2.meta and req1.headers == req2.headers

    enc = encoder(Request, send_body=True)
    dec = decoder(Request, Response)
    req = Request(url="http://www.yandex.ru", meta={"test": "shmest"}, headers={'reqhdr': 'value'})
    req2 = Request(url="http://www.yandex.ru/search")
    msgs = [
        enc.encode_add_seeds([req]),
        enc.encode_page_crawled(Response(url="http://www.yandex.ru", body='SOME CONTENT', headers={'hdr': 'value'},
                                         request=req), [req2]),
        enc.encode_request_error(req, "Host not found"),
        enc.encode_update_score("1be68ff556fd0bbe5802d1a100850da29f7f15b1", 0.51, "http://yandex.ru", True),
        enc.encode_new_job_id(1),
        enc.encode_offset(0, 28796),
        enc.encode_request(req)
    ]

    it = iter(msgs)

    o = dec.decode(it.next())
    assert o[0] == 'add_seeds'
    assert type(o[1]) == list
    req_d = o[1][0]
    check_request(req_d, req)
    assert type(req_d) == Request

    o = dec.decode(it.next())
    assert o[0] == 'page_crawled'
    assert type(o[1]) == Response
    assert o[1].url == req.url and o[1].body == 'SOME CONTENT' and o[1].meta == req.meta

    assert type(o[2]) == list
    req_d = o[2][0]
    assert type(req_d) == Request
    assert req_d.url == req2.url

    o_type, o_req, o_error = dec.decode(it.next())
    assert o_type == 'request_error'
    check_request(o_req, req)
    assert o_error == "Host not found"

    o_type, fprint, score, url, schedule = dec.decode(it.next())
    assert o_type == 'update_score'
    assert fprint == "1be68ff556fd0bbe5802d1a100850da29f7f15b1"
    assert score == 0.51
    assert url == "http://yandex.ru"
    assert schedule is True

    o_type, job_id = dec.decode(it.next())
    assert o_type == 'new_job_id'
    assert job_id == 1

    o_type, partition_id, offset = dec.decode(it.next())
    assert o_type == 'offset'
    assert partition_id == 0
    assert offset == 28796

    o = dec.decode_request(it.next())
    check_request(o, req)