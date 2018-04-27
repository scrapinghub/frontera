from math import ceil

import hashlib
from cachetools import LRUCache
from msgpack import Packer, unpackb
from random import randint
from six import MAXSIZE
from struct import pack


def random_bytes():
    return pack("L", randint(0, MAXSIZE))


class FramedTransport(object):
    def __init__(self, max_message_size):
        self.max_message_size = max_message_size
        self.buffer = LRUCache(10)
        self.packer = Packer()

    def read(self, kafka_msg):
        frame = unpackb(kafka_msg.value)
        seg_id, seg_count, msg_key, msg = frame
        if seg_count == 1:
            return msg

        buffer = self.buffer.get(msg_key, dict())
        if not buffer:
            self.buffer[msg_key] = buffer
        buffer[seg_id] = frame
        if len(buffer) == seg_count:
            msgs = [buffer[seg_id][3] for seg_id in sorted(buffer.keys())]
            final_msg = b''.join(msgs)
            del self.buffer[msg_key]
            return final_msg
        return None

    def write(self, key, msg):
        if len(msg) < self.max_message_size:
            yield self.packer.pack((0, 1, None, msg))
        else:
            length = len(msg)
            seg_size = self.max_message_size
            seg_count = int(ceil(length / float(seg_size)))
            h = hashlib.sha1()
            h.update(msg)
            h.update(random_bytes())
            msg_key = h.digest()
            for seg_id in range(seg_count):
                seg_msg = msg[seg_id * seg_size: (seg_id + 1) * seg_size]
                yield self.packer.pack((seg_id, seg_count, msg_key, seg_msg))