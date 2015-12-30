# -*- coding: utf-8 -*-


class SocketConfig(object):

    hostname = None
    base_port = None

    def __init__(self, hostname, base_port):
        self.hostname = hostname
        self.base_port = base_port

    def spiders_in(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port)

    def spiders_out(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port + 1)

    def sw_in(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port + 2)

    def sw_out(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port + 3)

    def db_in(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port + 4)

    def db_out(self):
        return 'tcp://%s:%d' % (self.hostname, self.base_port + 5)