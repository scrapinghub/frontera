# -*- coding: utf-8 -*-
from frontera.contrib.messagebus.kafka.transport import FramedTransport
import random
import string
from collections import namedtuple
import unittest

KafkaMessage = namedtuple("KafkaMessage", ['key', 'value'])


def get_blob(size):
    s = ''.join(random.choice(string.ascii_letters) for x in range(size))
    return s.encode("latin1")


class TestFramedTransport(unittest.TestCase):
    def setUp(self):
        self.transport = FramedTransport(32768)

    def test_big_message(self):
        test_msg = get_blob(1000000)
        assert len(test_msg) == 1000000
        framed_msgs = [m for m in self.transport.write(b"key", test_msg)]
        assert len(framed_msgs) == 31

        random.shuffle(framed_msgs)

        for i, msg in enumerate(framed_msgs):
            km = KafkaMessage(key=b"key", value=msg)
            result = self.transport.read(km)
            if i < len(framed_msgs) - 1:
                assert result is None
        assert result == test_msg   # the last one is triggering msg assembling

    def test_common_message(self):
        test_msg = get_blob(4096)
        framed_msgs = [m for m in self.transport.write(b"key", test_msg)]
        assert len(framed_msgs) == 1

        km = KafkaMessage(key=b"key", value=framed_msgs[0])
        result = self.transport.read(km)
        assert result == test_msg
