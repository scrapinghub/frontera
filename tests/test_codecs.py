import pytest

pytest.importorskip("msgpack")

import json
import unittest

from frontera.contrib.backends.remote.codecs.json import (
    Decoder as JsonDecoder,
)
from frontera.contrib.backends.remote.codecs.json import (
    Encoder as JsonEncoder,
)
from frontera.contrib.backends.remote.codecs.json import (
    _convert_and_save_type,
    _convert_from_saved_type,
)
from frontera.contrib.backends.remote.codecs.msgpack import (
    Decoder as MsgPackDecoder,
)
from frontera.contrib.backends.remote.codecs.msgpack import (
    Encoder as MsgPackEncoder,
)
from frontera.core.models import Request, Response


def _compare_dicts(dict1, dict2):
    """
     Compares two dicts
    :return: True if both dicts are equal else False
    """
    if dict1 is None or dict2 is None:
        return False

    if type(dict1) is not dict or type(dict2) is not dict:
        return False

    shared_keys = set(dict2.keys()) & set(dict2.keys())

    if not (
        len(shared_keys) == len(dict1.keys()) and len(shared_keys) == len(dict2.keys())
    ):
        return False

    dicts_are_equal = True
    for key in dict1:
        if type(dict1[key]) is dict:
            dicts_are_equal = _compare_dicts(dict1[key], dict2[key])
        else:
            dicts_are_equal = (dict1[key] == dict2[key]) and (
                type(dict1[key]) is type(dict2[key])
            )

        if not dicts_are_equal:
            return False

    return dicts_are_equal


@pytest.mark.parametrize("send_body", [True, False])
@pytest.mark.parametrize(
    ("encoder", "decoder"),
    [(MsgPackEncoder, MsgPackDecoder), (JsonEncoder, JsonDecoder)],
)
def test_codec(encoder, decoder, send_body):
    def check_request(req1, req2):
        assert (
            req1.url == req2.url
            and _compare_dicts(req1.meta, req2.meta) is True
            and _compare_dicts(req1.headers, req2.headers) is True
            and req1.method == req2.method
        )

    enc = encoder(Request, send_body=send_body)
    dec = decoder(Request, Response)
    req = Request(
        url="http://www.yandex.ru",
        method=b"GET",
        meta={b"test": b"shmest", b"scrapy_meta": {"rule": 0, "key": "value"}},
        headers={b"reqhdr": b"value"},
    )
    req2 = Request(url="http://www.yandex.ru/search")
    msgs = [
        enc.encode_add_seeds([req]),
        enc.encode_page_crawled(
            Response(
                url="http://www.yandex.ru",
                body=b"SOME CONTENT",
                headers={b"hdr": b"value"},
                request=req,
            )
        ),
        enc.encode_links_extracted(req, [req2]),
        enc.encode_request_error(req, "Host not found"),
        enc.encode_update_score(req, 0.51, True),
        enc.encode_new_job_id(1),
        enc.encode_offset(0, 28796),
        enc.encode_request(req),
    ]

    it = iter(msgs)

    o = dec.decode(next(it))
    assert o[0] == "add_seeds"
    assert type(o[1]) is list
    req_d = o[1][0]
    check_request(req_d, req)
    assert type(req_d) is Request

    o = dec.decode(next(it))
    assert o[0] == "page_crawled"
    assert type(o[1]) is Response
    assert o[1].url == req.url and o[1].meta == req.meta
    if send_body:
        assert o[1].body == b"SOME CONTENT"
    else:
        assert o[1].body is None

    o = dec.decode(next(it))
    print(o)
    assert o[0] == "links_extracted"
    assert type(o[1]) is Request
    assert o[1].url == req.url and o[1].meta == req.meta
    assert type(o[2]) is list
    req_d = o[2][0]
    assert type(req_d) is Request
    assert req_d.url == req2.url

    o_type, o_req, o_error = dec.decode(next(it))
    assert o_type == "request_error"
    check_request(o_req, req)
    assert o_error == "Host not found"

    o_type, o_req2, score, schedule = dec.decode(next(it))
    assert o_type == "update_score"
    assert (
        o_req2.url == req.url
        and o_req2.meta == req.meta
        and o_req2.headers == req.headers
    )
    assert score == 0.51
    assert schedule is True

    o_type, job_id = dec.decode(next(it))
    assert o_type == "new_job_id"
    assert job_id == 1

    o_type, partition_id, offset = dec.decode(next(it))
    assert o_type == "offset"
    assert partition_id == 0
    assert offset == 28796

    o = dec.decode_request(next(it))
    check_request(o, req)


class TestEncodeDecodeJson(unittest.TestCase):
    """
    Test for testing methods `_encode_recursively` and `_decode_recursively` used in json codec
    """

    def test_encode_decode_json_recursively(self):
        _int = 1
        _bytes = b"bytes"
        _unicode = "unicode"
        _bool = True
        _none = None
        simple_dict = {"key": "value"}
        simple_list = ["item", 1]
        simple_tuple = ("str", 2)
        mixed_type_dict = {
            b"k1": "v1",
            "k2": b"v2",
            "int": 1,
            b"none": None,
            "bool": False,
        }
        mixed_type_list = [b"i1", "i2", 23, None, True]
        mixed_type_tuple = [b"i1", "i2", 23, None, True]
        nested_dict = {
            "k1": b"v1",
            "lst": [b"i1", 1, ("str", 1, {"k2": b"v1", "tup": (1, None)})],
        }
        nested_list = [
            True,
            None,
            (1, 2, 3),
            {b"k1": b"v1", "tup": ("a", b"b", [None, False])},
        ]
        nested_tuple = (
            1,
            None,
            ["a", "b", True, {b"k1": "v2", "lst": ["a", False, (2, 3, 5)]}],
        )
        msgs = [
            _int,
            _bytes,
            _unicode,
            _bool,
            _none,
            simple_dict,
            simple_list,
            simple_tuple,
            mixed_type_dict,
            mixed_type_list,
            mixed_type_tuple,
            nested_dict,
            nested_list,
            nested_tuple,
        ]
        encoder = json.JSONEncoder()
        decoder = json.JSONDecoder()
        for original_msg in msgs:
            encoded_msg_1 = _convert_and_save_type(original_msg)
            encoded_msg_2 = encoder.encode(encoded_msg_1)
            decoded_msg_2 = decoder.decode(encoded_msg_2)
            decoded_msg_1 = _convert_from_saved_type(decoded_msg_2)
            if isinstance(decoded_msg_1, dict):
                self.assertDictEqual(decoded_msg_1, original_msg)
            elif isinstance(decoded_msg_1, (list, tuple)):
                self.assertSequenceEqual(decoded_msg_1, original_msg)
