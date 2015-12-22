# -*- coding: utf-8 -*-
from collections import namedtuple
from logging import getLogger

from kafka.common import OffsetRequest, check_error, OffsetFetchRequest, UnknownTopicOrPartitionError

logger = getLogger("offset-fetcher")
OffsetsStruct = namedtuple("OffsetsStruct", ["commit", "produced"])


class OffsetsFetcher(object):
    def __init__(self, client, topic, group_id):
        self._client = client
        self._topic = topic
        self._group_id = group_id
        self._client.load_metadata_for_topics()
        self._offsets = OffsetsStruct(commit=dict(),
                                      produced=dict())
        self._update_group_offsets()
        self._update_produced_offsets()

    def _update_produced_offsets(self):
        """
        Arguments:
            request_time_ms (int): Used to ask for all messages before a
                certain time (ms). There are two special values. Specify -1 to receive the latest
                offset (i.e. the offset of the next coming message) and -2 to receive the earliest
                available offset. Note that because offsets are pulled in descending order, asking for
                the earliest offset will always return you a single element.
        """
        for partition in self._client.get_partition_ids_for_topic(self._topic):
            reqs = [OffsetRequest(self._topic, partition, -1, 1)]

            (resp,) = self._client.send_offset_request(reqs)

            check_error(resp)
            assert resp.topic == self._topic
            assert resp.partition == partition
            self._offsets.produced[partition] = resp.offsets[0]

    def _update_group_offsets(self):
        logger.info("Consumer fetching stored offsets")
        for partition in self._client.get_partition_ids_for_topic(self._topic):
            (resp,) = self._client.send_offset_fetch_request(
                self._group_id,
                [OffsetFetchRequest(self._topic, partition)],
                fail_on_error=False)
            try:
                check_error(resp)
            except UnknownTopicOrPartitionError:
                pass

            if resp.offset == -1:
                self._offsets.commit[partition] = None
            else:
                self._offsets.commit[partition] = resp.offset

    def get(self):
        """
        :return: dict Lags per partition
        """
        self._update_produced_offsets()
        self._update_group_offsets()

        lags = {}
        for partition in self._client.get_partition_ids_for_topic(self._topic):
            produced = self._offsets.produced[partition]
            lag = produced - self._offsets.commit[partition] if self._offsets.commit[partition] else 0.0
            lags[partition] = lag
        return lags