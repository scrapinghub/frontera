from botocore.response import StreamingBody
from io import RawIOBase


class StreamingBodyIOBase(RawIOBase):
    def __init__(self, streaming_body, *args, **kwargs):
        assert isinstance(streaming_body, StreamingBody)
        self._sb = streaming_body
        super(StreamingBodyIOBase, self).__init__(*args, **kwargs)

    def close(self):
        self._sb.close()

    def read(self, size=-1):
        if size == -1:
            size = None
        return self._sb.read(size)

    def readable(self, *args, **kwargs):
        return self._sb._amount_read < self._sb._content_length

    def tell(self):
        return self._sb._amount_read

    def seekable(self, *args, **kwargs):
        return False

    def writable(self, *args, **kwargs):
        return False

    def isatty(self, *args, **kwargs):
        return False



