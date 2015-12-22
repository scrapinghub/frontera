from importlib import import_module
from zlib import crc32
from twisted.internet import reactor, error
from twisted.internet.defer import Deferred


def load_object(path):
    """Load an object given its absolute object path, and return it.

    object can be a class, function, variable o instance.
    path ie: 'myproject.frontier.models.Page'
    """

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError("Error loading object '%s': not a full path" % path)

    module, name = path[:dot], path[dot+1:]
    try:
        mod = import_module(module)
    except ImportError as e:
        raise ImportError("Error loading object '%s': %s" % (path, e))

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError("Module '%s' doesn't define any object named '%s'" % (module, name))

    return obj


def get_crc32(name):
    return crc32(name) if type(name) is str else crc32(name.encode('utf-8', 'ignore'))


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class CallLaterOnce(object):
    """Schedule a function to be called in the next reactor loop, but only if
    it hasn't been already scheduled since the last time it run.
    """
    def __init__(self, func, *a, **kw):
        self._func = func
        self._a = a
        self._kw = kw
        self._call = None
        self._errfunc = None
        self._erra = None
        self._errkw = None

    def setErrback(self, func, *a, **kw):
        self._errfunc = func
        self._erra = a
        self._errkw = kw

    def schedule(self, delay=0.0):
        if self._call is None:
            d = Deferred()
            d.addCallback(self)
            if self._errfunc:
                d.addErrback(self.error)
            self._call = reactor.callLater(delay, d.callback, None)

    def cancel(self):
        if self._call:
            self._call.cancel()

    def __call__(self, *args, **kwargs):
        self._call = None
        return self._func(*self._a, **self._kw)

    def error(self, f):
        self._call = None
        if self._errfunc:
            return self._errfunc(f, *self._erra, **self._errkw)
        return f


def listen_tcp(portrange, host, factory):
    """Like reactor.listenTCP but tries different ports in a range."""
    assert len(portrange) <= 2, "invalid portrange: %s" % portrange
    if not hasattr(portrange, '__iter__'):
        return reactor.listenTCP(portrange, factory, interface=host)
    if not portrange:
        return reactor.listenTCP(0, factory, interface=host)
    if len(portrange) == 1:
        return reactor.listenTCP(portrange[0], factory, interface=host)
    for x in range(portrange[0], portrange[1] + 1):
        try:
            return reactor.listenTCP(x, factory, interface=host)
        except error.CannotListenError:
            if x == portrange[1]:
                raise