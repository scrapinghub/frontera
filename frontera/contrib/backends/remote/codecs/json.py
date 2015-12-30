# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
from base64 import b64decode, b64encode
from frontera.core.codec import BaseDecoder, BaseEncoder


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

    def encode_add_seeds(self, seeds):
        return self.encode({
            'type': 'add_seeds',
            'seeds': [_prepare_request_message(seed) for seed in seeds]
        })

    def encode_page_crawled(self, response, links):
        return self.encode({
            'type': 'page_crawled',
            'r': _prepare_response_message(response, self.send_body),
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

    def encode_update_score(self, fingerprint, score, url, schedule):
        return self.encode({'type': 'update_score',
                            'fprint': fingerprint,
                            'score': score,
                            'url': url,
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
        request = self._request_model(url=obj['url'],
                                      meta=obj['meta'])
        return self._response_model(url=obj['url'],
                                    status_code=obj['status_code'],
                                    body=b64decode(obj['body']),
                                    request=request)

    def _request_from_object(self, obj):
        return self._request_model(url=obj['url'],
                                   method=obj['method'],
                                   headers=obj['headers'],
                                   cookies=obj['cookies'],
                                   meta=obj['meta'])

    def decode(self, message):
        message = super(Decoder, self).decode(message)
        if message['type'] == 'page_crawled':
            response = self._response_from_object(message['r'])
            links = [self._request_from_object(link) for link in message['links']]
            return ('page_crawled', response, links)
        if message['type'] == 'request_error':
            request = self._request_from_object(message['r'])
            return ('request_error', request, message['error'])
        if message['type'] == 'update_score':
            return ('update_score', str(message['fprint']), message['score'], str(message['url']), message['schedule'])
        if message['type'] == 'add_seeds':
            seeds = []
            for seed in message['seeds']:
                request = self._request_from_object(seed)
                seeds.append(request)
            return ('add_seeds', seeds)
        if message['type'] == 'new_job_id':
            return ('new_job_id', int(message['job_id']))
        if message['type'] == 'offset':
            return ('offset', int(message['partition_id']), int(message['offset']))
        return TypeError('Unknown message type')

    def decode_request(self, message):
        obj = super(Decoder, self).decode(message)
        return self._request_model(url=obj['url'],
                                   method=obj['method'],
                                   headers=obj['headers'],
                                   cookies=obj['cookies'],
                                   meta=obj['meta'])

