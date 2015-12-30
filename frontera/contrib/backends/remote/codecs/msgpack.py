# -*- coding: utf-8 -*-
from __future__ import absolute_import

from msgpack import packb, unpackb

from frontera.core.codec import BaseDecoder, BaseEncoder


def _prepare_request_message(request):
    def serialize(obj):
        """Recursively walk object's hierarchy."""
        if isinstance(obj, (bool, int, long, float, basestring)):
            return obj
        elif isinstance(obj, dict):
            obj = obj.copy()
            for key in obj:
                obj[key] = serialize(obj[key])
            return obj
        elif isinstance(obj, list):
            return [serialize(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(serialize([item for item in obj]))
        elif hasattr(obj, '__dict__'):
            return serialize(obj.__dict__)
        else:
            return None
    return [request.url, request.headers, request.cookies, serialize(request.meta)]


def _prepare_response_message(response, send_body):
    return [response.url, response.status_code, response.meta, response.body if send_body else None]


class Encoder(BaseEncoder):
    def __init__(self, request_model, *a, **kw):
        self.send_body = True if 'send_body' in kw and kw['send_body'] else False

    def encode_add_seeds(self, seeds):
        return packb(['as', map(_prepare_request_message, seeds)])

    def encode_page_crawled(self, response, links):
        return packb(['pc', _prepare_response_message(response, self.send_body), map(_prepare_request_message, links)])

    def encode_request_error(self, request, error):
        return packb(['re', _prepare_request_message(request), str(error)])

    def encode_request(self, request):
        return packb(_prepare_request_message(request))

    def encode_update_score(self, fingerprint, score, url, schedule):
        return packb(['us', fingerprint, score, url, schedule])

    def encode_new_job_id(self, job_id):
        return packb(['njid', int(job_id)])

    def encode_offset(self, partition_id, offset):
        return packb(['of', int(partition_id), int(offset)])


class Decoder(BaseDecoder):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model

    def _response_from_object(self, obj):
        return self._response_model(url=obj[0],
                                    status_code=obj[1],
                                    body=obj[3],
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
        if obj[0] == 'njid':
            return ('new_job_id', int(obj[1]))
        if obj[0] == 'of':
            return ('offset', int(obj[1]), int(obj[2]))
        return TypeError('Unknown message type')

    def decode_request(self, buffer):
        return self._request_from_object(unpackb(buffer))