from __future__ import absolute_import
import copy
import logging
import time
import collections
import six
from kafka.client_async import KafkaClient
from kafka import errors as Errors, TopicPartition
from kafka.future import Future
from kafka.protocol.commit import GroupCoordinatorRequest, OffsetFetchRequest
from kafka.protocol.offset import OffsetRequest
from kafka.structs import OffsetAndMetadata


log = logging.getLogger('offsets-fetcher')


class OffsetsFetcherAsync(object):

    DEFAULT_CONFIG = {
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 3000,
        'retry_backoff_ms': 100,
        'api_version': (0, 9),
        'metric_group_prefix': ''
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)
        self._client = KafkaClient(**self.config)
        self._coordinator_id = None
        self.group_id = configs['group_id']
        self.topic = configs['topic']

    def _ensure_coordinator_known(self):
        """Block until the coordinator for this group is known
        (and we have an active connection -- java client uses unsent queue).
        """
        while self._coordinator_unknown():

            # Prior to 0.8.2 there was no group coordinator
            # so we will just pick a node at random and treat
            # it as the "coordinator"
            if self.config['api_version'] < (0, 8, 2):
                self._coordinator_id = self._client.least_loaded_node()
                self._client.ready(self._coordinator_id)
                continue

            future = self._send_group_coordinator_request()
            self._client.poll(future=future)

            if future.failed():
                if isinstance(future.exception,
                              Errors.GroupCoordinatorNotAvailableError):
                    continue
                elif future.retriable():
                    metadata_update = self._client.cluster.request_update()
                    self._client.poll(future=metadata_update)
                else:
                    raise future.exception  # pylint: disable-msg=raising-bad-type

    def _coordinator_unknown(self):
        """Check if we know who the coordinator is and have an active connection

        Side-effect: reset _coordinator_id to None if connection failed

        Returns:
            bool: True if the coordinator is unknown
        """
        if self._coordinator_id is None:
            return True

        if self._client.is_disconnected(self._coordinator_id):
            self._coordinator_dead()
            return True

        return False

    def _coordinator_dead(self, error=None):
        """Mark the current coordinator as dead."""
        if self._coordinator_id is not None:
            log.warning("Marking the coordinator dead (node %s) for group %s: %s.",
                        self._coordinator_id, self.group_id, error)
            self._coordinator_id = None

    def _send_group_coordinator_request(self):
        """Discover the current coordinator for the group.

        Returns:
            Future: resolves to the node id of the coordinator
        """
        node_id = self._client.least_loaded_node()
        if node_id is None:
            return Future().failure(Errors.NoBrokersAvailable())

        log.debug("Sending group coordinator request for group %s to broker %s",
                  self.group_id, node_id)
        request = GroupCoordinatorRequest[0](self.group_id)
        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_group_coordinator_response, future)
        _f.add_errback(self._failed_request, node_id, request, future)
        return future

    def _handle_group_coordinator_response(self, future, response):
        log.debug("Received group coordinator response %s", response)
        if not self._coordinator_unknown():
            # We already found the coordinator, so ignore the request
            log.debug("Coordinator already known -- ignoring metadata response")
            future.success(self._coordinator_id)
            return

        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            ok = self._client.cluster.add_group_coordinator(self.group_id, response)
            if not ok:
                # This could happen if coordinator metadata is different
                # than broker metadata
                future.failure(Errors.IllegalStateError())
                return

            self._coordinator_id = response.coordinator_id
            log.info("Discovered coordinator %s for group %s",
                     self._coordinator_id, self.group_id)
            self._client.ready(self._coordinator_id)
            future.success(self._coordinator_id)
        elif error_type is Errors.GroupCoordinatorNotAvailableError:
            log.debug("Group Coordinator Not Available; retry")
            future.failure(error_type())
        elif error_type is Errors.GroupAuthorizationFailedError:
            error = error_type(self.group_id)
            log.error("Group Coordinator Request failed: %s", error)
            future.failure(error)
        else:
            error = error_type()
            log.error("Unrecognized failure in Group Coordinator Request: %s",
                      error)
            future.failure(error)

    def _failed_request(self, node_id, request, future, error):
        log.error('Error sending %s to node %s [%s]',
                  request.__class__.__name__, node_id, error)
        # Marking coordinator dead
        # unless the error is caused by internal client pipelining
        if not isinstance(error, (Errors.NodeNotReadyError,
                                  Errors.TooManyInFlightRequests)):
            self._coordinator_dead()
        future.failure(error)

    def offsets(self, partitions, timestamp):
        """Fetch a single offset before the given timestamp for the set of partitions.

        Blocks until offset is obtained, or a non-retriable exception is raised

        Arguments:
            partitions (iterable of TopicPartition) The partition that needs fetching offset.
            timestamp (int): timestamp for fetching offset. -1 for the latest
                available, -2 for the earliest available. Otherwise timestamp
                is treated as epoch seconds.

        Returns:
            dict: TopicPartition and message offsets
        """
        retries = 3
        while retries > 0:
            offsets = {}
            for future in self._send_offset_request(partitions, timestamp):
                self._client.poll(future=future)

                if future.succeeded():
                    for tp, offset in future.value:
                        offsets[tp] = offset
                    continue

                if not future.retriable():
                    raise future.exception  # pylint: disable-msg=raising-bad-type

                if future.exception.invalid_metadata:
                    refresh_future = self._client.cluster.request_update()
                    self._client.poll(future=refresh_future, sleep=True)
                    log.warning("Got exception %s and kept the loop", future.exception)
            if offsets:
                return offsets
            retries -= 1
            log.warning("Retrying the offsets fetch loop (%d retries left)", retries)
        log.error("Unsuccessful offsets retrieval")
        return {}

    def _send_offset_request(self, partitions, timestamp):
        """Fetch a single offset before the given timestamp for the partition.

        Arguments:
            partitions iterable of TopicPartition: partitions that needs fetching offset
            timestamp (int): timestamp for fetching offset

        Returns:
            list of Future: resolves to the corresponding offset
        """
        topic = partitions[0].topic
        nodes_per_partitions = {}
        for partition in partitions:
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None:
                log.debug("Partition %s is unknown for fetching offset,"
                          " wait for metadata refresh", partition)
                return [Future().failure(Errors.StaleMetadata(partition))]
            elif node_id == -1:
                log.debug("Leader for partition %s unavailable for fetching offset,"
                          " wait for metadata refresh", partition)
                return [Future().failure(Errors.LeaderNotAvailableError(partition))]
            nodes_per_partitions.setdefault(node_id, []).append(partition)

        # Client returns a future that only fails on network issues
        # so create a separate future and attach a callback to update it
        # based on response error codes

        futures = []
        for node_id, partitions in six.iteritems(nodes_per_partitions):
            request = OffsetRequest[0](
                -1, [(topic, [(partition.partition, timestamp, 1) for partition in partitions])]
            )
            future_request = Future()
            _f = self._client.send(node_id, request)
            _f.add_callback(self._handle_offset_response, partitions, future_request)

            def errback(e):
                log.error("Offset request errback error %s", e)
                future_request.failure(e)
            _f.add_errback(errback)
            futures.append(future_request)

        return futures

    def _handle_offset_response(self, partitions, future, response):
        """Callback for the response of the list offset call above.

        Arguments:
            partition (TopicPartition): The partition that was fetched
            future (Future): the future to update based on response
            response (OffsetResponse): response from the server

        Raises:
            AssertionError: if response does not match partition
        """
        topic, partition_info = response.topics[0]
        assert len(response.topics) == 1, (
            'OffsetResponse should only be for a single topic')
        partition_ids = set([part.partition for part in partitions])
        result = []
        for pi in partition_info:
            part, error_code, offsets = pi
            assert topic == partitions[0].topic and part in partition_ids, (
                'OffsetResponse partition does not match OffsetRequest partition')
            error_type = Errors.for_code(error_code)
            if error_type is Errors.NoError:
                assert len(offsets) == 1, 'Expected OffsetResponse with one offset'
                log.debug("Fetched offset %s for partition %d", offsets[0], part)
                result.append((TopicPartition(topic, part), offsets[0]))
            elif error_type in (Errors.NotLeaderForPartitionError,
                                Errors.UnknownTopicOrPartitionError):
                log.debug("Attempt to fetch offsets for partition %s failed due"
                          " to obsolete leadership information, retrying.",
                          str(partitions))
                future.failure(error_type(partitions))
            else:
                log.warning("Attempt to fetch offsets for partition %s failed due to:"
                            " %s", partitions, error_type)
                future.failure(error_type(partitions))
        future.success(result)

    def fetch_committed_offsets(self, partitions):
        """Fetch the current committed offsets for specified partitions

        Arguments:
            partitions (list of TopicPartition): partitions to fetch

        Returns:
            dict: {TopicPartition: OffsetAndMetadata}
        """
        if not partitions:
            return {}

        while True:
            self._ensure_coordinator_known()

            # contact coordinator to fetch committed offsets
            future = self._send_offset_fetch_request(partitions)
            self._client.poll(future=future)

            if future.succeeded():
                return future.value

            if not future.retriable():
                raise future.exception  # pylint: disable-msg=raising-bad-type

            time.sleep(self.config['retry_backoff_ms'] / 1000.0)

    def _send_offset_fetch_request(self, partitions):
        """Fetch the committed offsets for a set of partitions.

        This is a non-blocking call. The returned future can be polled to get
        the actual offsets returned from the broker.

        Arguments:
            partitions (list of TopicPartition): the partitions to fetch

        Returns:
            Future: resolves to dict of offsets: {TopicPartition: int}
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), partitions))
        if not partitions:
            return Future().success({})

        elif self._coordinator_unknown():
            return Future().failure(Errors.GroupCoordinatorNotAvailableError)

        node_id = self._coordinator_id

        # Verify node is ready
        if not self._client.ready(node_id):
            log.debug("Node %s not ready -- failing offset fetch request",
                      node_id)
            return Future().failure(Errors.NodeNotReadyError)

        log.debug("Group %s fetching committed offsets for partitions: %s",
                  self.group_id, partitions)
        # construct the request
        topic_partitions = collections.defaultdict(set)
        for tp in partitions:
            topic_partitions[tp.topic].add(tp.partition)

        if self.config['api_version'] >= (0, 8, 2):
            request = OffsetFetchRequest[1](
                self.group_id,
                list(topic_partitions.items())
            )
        else:
            request = OffsetFetchRequest[0](
                self.group_id,
                list(topic_partitions.items())
            )

        # send the request with a callback
        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_offset_fetch_response, future)
        _f.add_errback(self._failed_request, node_id, request, future)
        return future

    def _handle_offset_fetch_response(self, future, response):
        offsets = {}
        for topic, partitions in response.topics:
            for partition, offset, metadata, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    error = error_type()
                    log.debug("Group %s failed to fetch offset for partition"
                              " %s: %s", self.group_id, tp, error)
                    if error_type is Errors.GroupLoadInProgressError:
                        # just retry
                        future.failure(error)
                    elif error_type is Errors.NotCoordinatorForGroupError:
                        # re-discover the coordinator and retry
                        self._coordinator_dead()
                        future.failure(error)
                    elif error_type in (Errors.UnknownMemberIdError,
                                        Errors.IllegalGenerationError):
                        future.failure(error)
                    elif error_type is Errors.UnknownTopicOrPartitionError:
                        log.warning("OffsetFetchRequest -- unknown topic %s"
                                    " (have you committed any offsets yet?)",
                                    topic)
                        continue
                    else:
                        log.error("Unknown error fetching offsets for %s: %s",
                                  tp, error)
                        future.failure(error)
                    return
                elif offset >= 0:
                    # record the position with the offset
                    # (-1 indicates no committed offset to fetch)
                    offsets[tp] = OffsetAndMetadata(offset, metadata)
                else:
                    log.debug("Group %s has no committed offset for partition"
                              " %s", self.group_id, tp)
        future.success(offsets)

    def get(self):
        topic_partitions = self._client.cluster.partitions_for_topic(self.topic)
        if not topic_partitions:
            future = self._client.cluster.request_update()
            log.info("No partitions available, performing metadata update.")
            self._client.poll(future=future)
            return {}
        partitions = [TopicPartition(self.topic, partition_id) for partition_id in topic_partitions]
        offsets = self.offsets(partitions, -1)
        committed = self.fetch_committed_offsets(partitions)
        lags = {}
        for tp, offset in six.iteritems(offsets):
            commit_offset = committed[tp] if tp in committed else 0
            numerical = commit_offset if isinstance(commit_offset, int) else commit_offset.offset
            lag = offset - numerical
            pid = tp.partition if isinstance(tp, TopicPartition) else tp
            log.debug("Lag for %s (%s): %s, %s, %s", self.topic, pid, offset, commit_offset, lag)
            lags[pid] = lag
        return lags