# -*- coding: utf-8 -*-
from logging import getLogger
from json import JSONDecoder, JSONEncoder
from sys import exc_info
from traceback import format_exception

from twisted.web import server, resource

from frontera.utils.async import listen_tcp

logger = getLogger("cf-server")


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


class JsonRpcError(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __call__(self, id):
        return jsonrpc_error(id, self.code, self.message)


class JsonResource(resource.Resource):

    json_encoder = JSONEncoder()
    json_decoder = JSONDecoder()

    def render(self, txrequest):
        r = resource.Resource.render(self, txrequest)
        return self.render_object(r, txrequest)

    def render_object(self, obj, txrequest):
        r = self.json_encoder.encode(obj) + "\n"
        txrequest.setHeader('Content-Type', 'application/json')
        txrequest.setHeader('Access-Control-Allow-Origin', '*')
        txrequest.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, PUT, DELETE')
        txrequest.setHeader('Access-Control-Allow-Headers', 'X-Requested-With')
        txrequest.setHeader('Content-Length', len(r))
        return r

    def parse_jsonrpc(self, txrequest):
        if isinstance(txrequest.content, file):
            data = txrequest.content.read()
        else:
            data = txrequest.content.getvalue()
        return self.json_decoder.decode(data)


class StatusResource(JsonResource):

    ws_name = 'status'

    def __init__(self, worker):
        self.worker = worker
        JsonResource.__init__(self)

    def render_GET(self, txrequest):
        return {
            'is_finishing': self.worker.slot.is_finishing,
            'disable_new_batches': self.worker.slot.disable_new_batches,
            'stats': self.worker.stats
        }


class JsonRpcResource(JsonResource):

    ws_name = 'jsonrpc'

    def __init__(self):
        JsonResource.__init__(self)

    def render_POST(self, txrequest):
        jrequest = self.parse_jsonrpc(txrequest)
        method = jrequest['method']
        try:
            try:
                return self.process_request(method, jrequest)
            except Exception, err:
                if isinstance(err, JsonRpcError):
                    raise err
                trace_lines = format_exception(*exc_info())
                raise JsonRpcError(500, "Error processing request: %s" % (str("").join(trace_lines)))
        except JsonRpcError, err:
            return err(jrequest['id'])


class WorkerJsonRpcResource(JsonRpcResource):

    def __init__(self, worker):
        self.worker = worker
        JsonRpcResource.__init__(self)

    def process_request(self, method, jrequest):
        if method == 'disable_new_batches':
            self.worker.disable_new_batches()
            return jsonrpc_result(jrequest['id'], "success")

        if method == 'enable_new_batches':
            self.worker.enable_new_batches()
            return jsonrpc_result(jrequest['id'], "success")
        raise JsonRpcError(400, "Unknown method")


class RootResource(JsonResource):

    def render_GET(self, txrequest):
        return {'resources': self.children.keys()}

    def getChild(self, name, txrequest):
        if name == '':
            return self
        return JsonResource.getChild(self, name, txrequest)


class JsonRpcService(server.Site):
    def __init__(self, root, settings):
        logfile = settings.get('JSONRPC_LOGFILE')
        self.portrange = settings.get('JSONRPC_PORT', [6023, 6073])
        self.host = settings.get('JSONRPC_HOST', '127.0.0.1')

        server.Site.__init__(self, root, logPath=logfile)
        self.noisy = False

    def start_listening(self):
        self.port = listen_tcp(self.portrange, self.host, self)
        h = self.port.getHost()
        logger.info('Web service listening on %(host)s:%(port)d'.format(host=h.host, port=h.port))

    def stop_listening(self):
        self.port.stopListening()


class WorkerJsonRpcService(JsonRpcService):
    def __init__(self, worker, settings):
        root = RootResource()
        root.putChild('status', StatusResource(worker))
        root.putChild('jsonrpc', WorkerJsonRpcResource(worker))
        JsonRpcService.__init__(self, root, settings)
        self.worker = worker

    def start_listening(self):
        JsonRpcService.start_listening(self)
        address = self.port.getHost()
        self.worker.set_process_info("%s:%d" % (address.host, address.port))
