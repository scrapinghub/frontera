# -*- coding: utf-8 -*-
""" A MsgPack codec for Frontera. Implemented using native msgpack-python library.
"""
from __future__ import absolute_import
import logging
from msgpack import packb, unpackb

from frontera.core.codec import BaseDecoder, BaseEncoder
import six
from w3lib.util import to_native_str


logger = logging.getLogger(__name__)


def _prepare_request_message(request):
    def serialize(obj):
        """Recursively walk object's hierarchy."""
        if isinstance(obj, (bool, six.integer_types, float, six.binary_type, six.text_type)):
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
            logger.warning('unable to serialize object: {}'.format(obj))
            return None
    return [request.url, request.method, request.headers, request.cookies, serialize(request.meta)]


def _prepare_response_message(response, send_body):
    return [response.url, response.status_code, response.meta, response.body if send_body else None]


class Encoder(BaseEncoder):
    def __init__(self, request_model, *a, **kw):
        self.send_body = True if 'send_body' in kw and kw['send_body'] else False

    def encode_add_seeds(self, seeds):
        return packb([b'as', [_prepare_request_message(seed) for seed in seeds]], use_bin_type=True)

    def encode_page_crawled(self, response):
        return packb([b'pc', _prepare_response_message(response, self.send_body)], use_bin_type=True)

    def encode_links_extracted(self, request, links):
        return packb([b'le', _prepare_request_message(request), [_prepare_request_message(link) for link in links]],
                     use_bin_type=True)

    def encode_request_error(self, request, error):
        return packb([b're', _prepare_request_message(request), str(error)], use_bin_type=True)

    def encode_request(self, request):
        return packb(_prepare_request_message(request), use_bin_type=True)

    def encode_update_score(self, request, score, schedule):
        return packb([b'us', _prepare_request_message(request), score, schedule], use_bin_type=True)

    def encode_new_job_id(self, job_id):
        return packb([b'njid', int(job_id)], use_bin_type=True)

    def encode_offset(self, partition_id, offset):
        return packb([b'of', int(partition_id), int(offset)], use_bin_type=True)


class Decoder(BaseDecoder):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model

    def _response_from_object(self, obj):
        url = to_native_str(obj[0])
        return self._response_model(url=url,
                                    status_code=obj[1],
                                    body=obj[3],
                                    request=self._request_model(url=url,
                                                                meta=obj[2]))

    def _request_from_object(self, obj):
        return self._request_model(url=to_native_str(obj[0]),
                                   method=obj[1],
                                   headers=obj[2],
                                   cookies=obj[3],
                                   meta=obj[4])

    def decode(self, buffer):
        obj = unpackb(buffer, encoding='utf-8')
        if obj[0] == b'pc':
            return ('page_crawled',
                    self._response_from_object(obj[1]))
        if obj[0] == b'le':
            return ('links_extracted',
                    self._request_from_object(obj[1]),
                    [self._request_from_object(x) for x in obj[2]])
        if obj[0] == b'us':
            return ('update_score', self._request_from_object(obj[1]), obj[2], obj[3])
        if obj[0] == b're':
            return ('request_error', self._request_from_object(obj[1]), to_native_str(obj[2]))
        if obj[0] == b'as':
            return ('add_seeds', [self._request_from_object(x) for x in obj[1]])
        if obj[0] == b'njid':
            return ('new_job_id', int(obj[1]))
        if obj[0] == b'of':
            return ('offset', int(obj[1]), int(obj[2]))
        return TypeError('Unknown message type')

    def decode_request(self, buffer):
        return self._request_from_object(unpackb(buffer, encoding='utf-8'))
