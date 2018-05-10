# -*- coding: utf-8 -*-
""" A JSON codec for Frontera. Implemented using native json library.
"""
from __future__ import absolute_import
import json
import six
from base64 import b64decode, b64encode
from frontera.core.codec import BaseDecoder, BaseEncoder
from w3lib.util import to_unicode, to_bytes


def _convert_and_save_type(obj):
    """
    :param obj: dict object

    The purpose of this method is to transform the given dict
    into a form that would be able to serialize with JSONEncoder.
    In order to implement this, this method converts all byte strings
    inside a dict to unicode and saves their type for reverting to its
    original state. The type and the value are stored as a tuple in the
    following format: (original_type, converted value). All other objects
    like dict, tuple, list are converted to the same format for the sake
    of serialization and for the ease of reverting.
    Refer `https://github.com/scrapinghub/frontera/pull/233#discussion_r97432868`
    for the detailed explanation about the design.
    """
    if isinstance(obj, bytes):
        return 'bytes', to_unicode(obj)
    elif isinstance(obj, dict):
        return 'dict', [(_convert_and_save_type(k), _convert_and_save_type(v)) for k, v in six.iteritems(obj)]
    elif isinstance(obj, (list, tuple)):
        return type(obj).__name__, [_convert_and_save_type(item) for item in obj]
    return 'other', obj


def _convert_from_saved_type(obj):
    """
    :param obj: object returned by `_convert_and_save_type`

    Restores the original state of the object converted
    earlier by `_convert_and_save_type`. This method considers every
    first element of the nested tuple as the original type information and
    the second value to be the converted value. It applies the original type
    recursively on the object to retrieve the original form of the object.
    """
    assert len(obj) == 2
    obj_type, obj_value = obj
    if obj_type == 'bytes':
        return to_bytes(obj_value)
    elif obj_type == 'dict':
        return dict([(_convert_from_saved_type(k), _convert_from_saved_type(v)) for k, v in obj_value])
    elif obj_type in ['list', 'tuple']:
        _type = list if obj_type == 'list' else tuple
        return _type([_convert_from_saved_type(item) for item in obj_value])
    return obj_value


def _prepare_request_message(request):
    return {'url': request.url,
            'method': request.method,
            'headers': request.headers,
            'cookies': request.cookies,
            'meta': request.meta}


def _prepare_links_message(links):
    return [_prepare_request_message(link) for link in links]


def _prepare_response_message(response, send_body):
    return {'url': response.url,
            'status_code': response.status_code,
            'meta': response.meta,
            'body': b64encode(response.body) if send_body else None}


class CrawlFrontierJSONEncoder(json.JSONEncoder):
    def __init__(self, request_model, *a, **kw):
        self._request_model = request_model
        super(CrawlFrontierJSONEncoder, self).__init__(*a, **kw)

    def default(self, o):
        if isinstance(o, self._request_model):
            return _prepare_request_message(o)
        else:
            return super(CrawlFrontierJSONEncoder, self).default(o)


class Encoder(BaseEncoder, CrawlFrontierJSONEncoder):
    def __init__(self, request_model, *a, **kw):
        self.send_body = kw.pop('send_body', False)
        super(Encoder, self).__init__(request_model, *a, **kw)

    def encode(self, obj):
        encoded = _convert_and_save_type(obj)
        return super(Encoder, self).encode(encoded)

    def encode_page_crawled(self, response):
        return self.encode({
            'type': 'page_crawled',
            'r': _prepare_response_message(response, self.send_body)
        })

    def encode_links_extracted(self, request, links):
        return self.encode({
            'type': 'links_extracted',
            'r': _prepare_request_message(request),
            'links': _prepare_links_message(links)
        })

    def encode_request_error(self, request, error):
        return self.encode({
            'type': 'request_error',
            'r': _prepare_request_message(request),
            'error': error
        })

    def encode_request(self, request):
        return self.encode(_prepare_request_message(request))

    def encode_update_score(self, request, score, schedule):
        return self.encode({'type': 'update_score',
                            'r': _prepare_request_message(request),
                            'score': score,
                            'schedule': schedule})

    def encode_new_job_id(self, job_id):
        return self.encode({
            'type': 'new_job_id',
            'job_id': int(job_id)
        })

    def encode_offset(self, partition_id, offset):
        return self.encode({
            'type': 'offset',
            'partition_id': int(partition_id),
            'offset': int(offset)
        })

    def encode_stats(self, stats):
        return self.encode({
            'type': 'stats',
            'stats': stats
        })


class Decoder(json.JSONDecoder, BaseDecoder):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model
        super(Decoder, self).__init__(*a, **kw)

    def _response_from_object(self, obj):
        url = obj['url']
        request = self._request_model(url=url,
                                      meta=obj['meta'])
        return self._response_model(url=url,
                                    status_code=obj['status_code'],
                                    body=b64decode(obj['body']) if obj['body'] is not None else None,
                                    request=request)

    def _request_from_object(self, obj):
        return self._request_model(url=obj['url'],
                                   method=obj['method'],
                                   headers=obj['headers'],
                                   cookies=obj['cookies'],
                                   meta=obj['meta'])

    def decode(self, message):
        message = _convert_from_saved_type(super(Decoder, self).decode(message))
        if message['type'] == 'links_extracted':
            request = self._request_from_object(message['r'])
            links = [self._request_from_object(link) for link in message['links']]
            return ('links_extracted', request, links)
        if message['type'] == 'page_crawled':
            response = self._response_from_object(message['r'])
            return ('page_crawled', response)
        if message['type'] == 'request_error':
            request = self._request_from_object(message['r'])
            return ('request_error', request, message['error'])
        if message['type'] == 'update_score':
            return ('update_score', self._request_from_object(message['r']), message['score'], message['schedule'])
        if message['type'] == 'new_job_id':
            return ('new_job_id', int(message['job_id']))
        if message['type'] == 'offset':
            return ('offset', int(message['partition_id']), int(message['offset']))
        if message['type'] == 'stats':
            return ('stats', message['stats'])
        raise TypeError('Unknown message type')

    def decode_request(self, message):
        obj = _convert_from_saved_type(super(Decoder, self).decode(message))
        return self._request_model(url=obj['url'],
                                   method=obj['method'],
                                   headers=obj['headers'],
                                   cookies=obj['cookies'],
                                   meta=obj['meta'])