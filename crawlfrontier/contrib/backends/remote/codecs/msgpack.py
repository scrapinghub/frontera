# -*- coding: utf-8 -*-
from __future__ import absolute_import
from msgpack import packb, unpackb

def _prepare_request_message(request):
    return [request.url, request.headers, request.cookies, request.meta]

def _prepare_response_message(response):
    return [response.url, response.status_code, response.meta]


class Encoder(object):
    def __init__(self, request_model, *a, **kw):
        pass

    def encode_add_seeds(self, seeds):
        return packb(['as', map(_prepare_request_message, seeds)])

    def encode_page_crawled(self, response, links):
        return packb(['pc', _prepare_response_message(response), map(_prepare_request_message, links)])

    def encode_request_error(self, request, error):
        return packb(['re', _prepare_request_message(request), str(error)])

    def encode_request(self, request):
        return packb(_prepare_request_message(request))

    def encode_update_score(self, fingerprint, score, url, schedule):
        return packb(['us', fingerprint, score, url, schedule])


class Decoder(object):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model

    def _response_from_object(self, obj):
        return self._response_model(url=obj[0],
                                    status_code=obj[1],
                                    request=self._request_model(url=obj[0],
                                                                meta=obj[2]))

    def _request_from_object(self, obj):
        return self._request_model(url=obj[0],
                                   headers=obj[1],
                                   cookies=obj[2],
                                   meta=obj[3])

    def decode(self, buffer):
        obj = unpackb(buffer)
        if obj[0] == 'pc':
            return ('page_crawled',
                    self._response_from_object(obj[1]),
                    map(self._request_from_object, obj[2]))
        if obj[0] == 'us':
            return ('update_score', str(obj[1]), obj[2], str(obj[3]), obj[4])
        if obj[0] == 're':
            return ('request_error', self._request_from_object(obj[1]), obj[2])
        if obj[0] == 'as':
            return ('add_seeds', map(self._request_from_object, obj[1]))
        return TypeError('Unknown message type')

    def decode_request(self, buffer):
        return self._request_from_object(unpackb(buffer))