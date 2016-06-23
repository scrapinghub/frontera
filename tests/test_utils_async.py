# -*- coding: utf-8 -*-
import pytest
from twisted.test.proto_helpers import MemoryReactor
from twisted.internet.protocol import Factory
from twisted.internet.task import Clock
from frontera.utils.async import CallLaterOnce, listen_tcp


class TestCallLaterOnce(object):

    called = 0

    def call_function(self):
        self.called += 1

    def test_call_later_once(self):
        self.called = 0
        reactor = Clock()
        call = CallLaterOnce(self.call_function, reactor=reactor)
        call.schedule(delay=1)
        reactor.advance(1)
        assert self.called == 1

    def test_call_later_twice(self):
        self.called = 0
        reactor = Clock()
        call = CallLaterOnce(self.call_function, reactor=reactor)
        call.schedule(delay=1)
        reactor.advance(1)
        call.schedule(delay=1)
        reactor.advance(1)
        assert self.called == 2

    def test_call_later_twice_already_scheduled(self):
        self.called = 0
        reactor = Clock()
        call = CallLaterOnce(self.call_function, reactor=reactor)
        call.schedule(delay=1)
        call.schedule(delay=2)
        reactor.advance(2)
        assert self.called == 1

    def test_call_later_cancel(self):
        self.called = 0
        reactor = Clock()
        call = CallLaterOnce(self.call_function, reactor=reactor)
        call.schedule(delay=1)
        call.cancel()
        reactor.advance(1)
        assert self.called == 0


class TestListenTCP(object):

    host = '127.0.0.1'
    port = 6023
    portrange = [6023, 6073]

    def test_listen_tcp_integer(self):
        reactor = MemoryReactor()
        result = listen_tcp(self.port, self.host, Factory, reactor=reactor)
        assert result.getHost().port == self.port
        assert reactor.tcpServers[0][0] == self.port

    def test_listen_tcp_invalid_port_range(self):
        reactor = MemoryReactor()
        with pytest.raises(Exception) as info:
            listen_tcp([1, 2, 3], self.host, Factory, reactor=reactor)
        assert info.value.message == 'invalid portrange: [1, 2, 3]'

    def test_listen_tcp_default(self):
        reactor = MemoryReactor()
        result = listen_tcp([], self.host, Factory, reactor=reactor)
        assert result.getHost().port == 0
        assert reactor.tcpServers[0][0] == 0

    def test_listen_tcp_range_length_one(self):
        reactor = MemoryReactor()
        result = listen_tcp([self.port], self.host, Factory, reactor=reactor)
        assert result.getHost().port == self.port
        assert reactor.tcpServers[0][0] == self.port

    def test_listen_tcp_full_range(self):
        reactor = MemoryReactor()
        result = listen_tcp(self.portrange, self.host, Factory, reactor=reactor)
        assert self.portrange[0] <= result.getHost().port <= self.portrange[1]
        assert len(reactor.tcpServers) == 1
        assert self.portrange[0] <= reactor.tcpServers[0][0] <= self.portrange[1]
