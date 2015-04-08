# -*- coding: utf-8 -*-
from twisted.web import server, resource
from twisted.internet import reactor
import logging
import json

logger = logging.getLogger("cf-server")


def jsonrpc_error(id, code, message, data=None):
    """Create JSON-RPC error response"""
    return {
        'jsonrpc': '2.0',
        'error': {
            'code': code,
            'message': message,
            'data': data,
        },
        'id': id,
    }


def jsonrpc_result(id, result):
    """Create JSON-RPC result response"""
    return {
        'jsonrpc': '2.0',
        'result': result,
        'id': id,
    }


class JsonResource(resource.Resource):

    json_encoder = json.JSONEncoder()

    def render(self, txrequest):
        r = resource.Resource.render(self, txrequest)
        return self.render_object(r, txrequest)

    def render_object(self, obj, txrequest):
        r = self.json_encoder.encode(obj) + "\n"
        txrequest.setHeader('Content-Type', 'application/json')
        txrequest.setHeader('Access-Control-Allow-Origin', '*')
        txrequest.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, PUT, DELETE')
        txrequest.setHeader('Access-Control-Allow-Headers',' X-Requested-With')
        txrequest.setHeader('Content-Length', len(r))
        return r


class StatusResource(JsonResource):

    ws_name = 'status'

    def __init__(self, worker):
        self.worker = worker
        JsonResource.__init__(self)

    def render_GET(self, txrequest):
        return {
            'is_finishing': self.worker.slot.is_finishing,
            'stats': self.worker.stats
        }


class RootResource(JsonResource):

    def render_GET(self, txrequest):
        return {'resources': self.children.keys()}

    def getChild(self, name, txrequest):
        if name == '':
            return self
        return JsonResource.getChild(self, name, txrequest)


class JsonRpcService(server.Site):
    def __init__(self, worker, settings):
        logfile = settings.get('JSONRPC_LOGFILE')
        self.portrange = settings.get('JSONRPC_PORT', 6023)
        self.host = settings.get('JSONRPC_HOST', '127.0.0.1')

        root = RootResource()
        root.putChild('status', StatusResource(worker))

        server.Site.__init__(self, root, logPath=logfile)
        self.noisy = False

    def start_listening(self):
        self.port = reactor.listenTCP(self.portrange, self, interface=self.host)
        h = self.port.getHost()
        logger.info('Web service listening on %(host)s:%(port)d'.format(host=h.host, port=h.port))

    def stop_listening(self):
        self.port.stopListening()