# -*- coding: utf-8 -*-
""" A JSON codec for Frontera. Implemented using native json library.
"""
from __future__ import absolute_import
import json
from base64 import b64decode, b64encode
from frontera.core.codec import BaseDecoder, BaseEncoder
from w3lib.util import to_unicode, to_native_str
from frontera.utils.misc import dict_to_unicode, dict_to_bytes


def _prepare_request_message(request):
    return {'url': to_unicode(request.url),
            'method': to_unicode(request.method),
            'headers': dict_to_unicode(request.headers),
            'cookies': dict_to_unicode(request.cookies),
            'meta': dict_to_unicode(request.meta)}


def _prepare_links_message(links):
    return [_prepare_request_message(link) for link in links]


def _prepare_response_message(response, send_body):
    return {'url': to_unicode(response.url),
            'status_code': response.status_code,
            'meta': dict_to_unicode(response.meta),
            'body': to_unicode(b64encode(response.body)) if send_body else None}


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

    def encode_add_seeds(self, seeds):
        return self.encode({
            'type': 'add_seeds',
            'seeds': [_prepare_request_message(seed) for seed in seeds]
        })

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


class Decoder(json.JSONDecoder, BaseDecoder):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model
        super(Decoder, self).__init__(*a, **kw)

    def _response_from_object(self, obj):
        url = to_native_str(obj[b'url'])
        request = self._request_model(url=url,
                                      meta=obj[b'meta'])
        return self._response_model(url=url,
                                    status_code=obj[b'status_code'],
                                    body=b64decode(obj[b'body']),
                                    request=request)

    def _request_from_object(self, obj):
        return self._request_model(url=to_native_str(obj[b'url']),
                                   method=obj[b'method'],
                                   headers=obj[b'headers'],
                                   cookies=obj[b'cookies'],
                                   meta=obj[b'meta'])

    def decode(self, message):
        message = dict_to_bytes(super(Decoder, self).decode(message))
        if message[b'type'] == b'links_extracted':
            request = self._request_from_object(message[b'r'])
            links = [self._request_from_object(link) for link in message[b'links']]
            return ('links_extracted', request, links)
        if message[b'type'] == b'page_crawled':
            response = self._response_from_object(message[b'r'])
            return ('page_crawled', response)
        if message[b'type'] == b'request_error':
            request = self._request_from_object(message[b'r'])
            return ('request_error', request, to_native_str(message[b'error']))
        if message[b'type'] == b'update_score':
            return ('update_score', self._request_from_object(message[b'r']), message[b'score'], message[b'schedule'])
        if message[b'type'] == b'add_seeds':
            seeds = []
            for seed in message[b'seeds']:
                request = self._request_from_object(seed)
                seeds.append(request)
            return ('add_seeds', seeds)
        if message[b'type'] == b'new_job_id':
            return ('new_job_id', int(message[b'job_id']))
        if message[b'type'] == b'offset':
            return ('offset', int(message[b'partition_id']), int(message[b'offset']))
        return TypeError('Unknown message type')

    def decode_request(self, message):
        obj = dict_to_bytes(super(Decoder, self).decode(message))
        return self._request_model(url=to_native_str(obj[b'url']),
                                   method=obj[b'method'],
                                   headers=obj[b'headers'],
                                   cookies=obj[b'cookies'],
                                   meta=obj[b'meta'])

