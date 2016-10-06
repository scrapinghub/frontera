from frontera.contrib.backends.remote.codecs.msgpack import Decoder
from frontera.contrib.backends.remote.codecs.msgpack import Encoder
from frontera.core.models import Request


class DumbResponse:
    pass


class TestMsgpackMessageBus(object):

    def test_request_scrapy_meta_is_preserved(self):
        meta = {b'frontera_key': b'frontera_value',
                b'scrapy_meta': {'rule': 0, 'link': 'some_link'}}
        r = Request('http://example.com',
                    meta=meta)
        encoder = Encoder(Request)
        decoder = Decoder(Request, DumbResponse)
        response_meta = decoder.decode_request(encoder.encode_request(r)).meta
        assert response_meta == meta
