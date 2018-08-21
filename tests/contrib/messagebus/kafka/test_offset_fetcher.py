from mock import Mock, patch
from frontera.contrib.messagebus.kafka.offsets_fetcher import OffsetsFetcherAsync


class TestOffsetsFetcherAsync(object):

    def test_offsets_invalid_metadata(self):
        fetcher = OffsetsFetcherAsync(group_id='test', topic='test')
        future = Mock(succeeded=lambda: False, retriable=lambda: True, exceptions=Mock(invalid_metadata=True))
        with patch.object(fetcher, '_send_offset_request', return_value=[future]) as _send_offset_request:
            fetcher.offsets([0], -1)
            assert _send_offset_request.was_called()
