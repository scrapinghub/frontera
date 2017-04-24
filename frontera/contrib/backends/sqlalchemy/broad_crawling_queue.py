from __future__ import absolute_import

import logging

import six

from frontera.core.models import Request

from .queue import Queue
from .utils import retry_and_rollback


class BroadCrawlingQueue(Queue):

    GET_RETRIES = 3

    @retry_and_rollback
    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        Priorities, from highest to lowest:
         - max_requests_per_host
         - max_n_requests
         - min_hosts & min_requests

        :param max_n_requests:
        :param partition_id:
        :param kwargs: min_requests, min_hosts, max_requests_per_host
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop("min_requests", None)
        min_hosts = kwargs.pop("min_hosts", None)
        max_requests_per_host = kwargs.pop("max_requests_per_host", None)

        assert max_n_requests > min_requests

        queue = {}
        limit = max_n_requests
        tries = 0
        count = 0

        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0

            self.logger.debug(
                "Try %d, limit %d, last attempt: requests %d, hosts %d",
                tries, limit, count, len(queue.keys()),
            )
            queue.clear()

            count = 0
            query = self.session.query(self.queue_model).filter_by(partition_id=partition_id)

            for item in self._order_by(query).limit(limit):

                if item.host_crc32 not in queue:
                    queue[item.host_crc32] = []

                if max_requests_per_host is not None:
                    if len(queue[item.host_crc32]) > max_requests_per_host:
                        continue

                queue[item.host_crc32].append(item)

                count += 1

                if count > max_n_requests:
                    break

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if min_requests is not None and count < min_requests:
                continue

            break

        self.logger.debug(
            "Finished: tries %d, hosts %d, requests %d", tries,
            len(queue.keys()), count,
        )

        results = []

        for items in six.itervalues(queue):
            for item in items:
                method = item.method or b'GET'

                results.append(Request(
                    item.url, method=method, meta=item.meta,
                    headers=item.headers, cookies=item.cookies,
                ))
                self.session.delete(item)

        self.session.commit()

        return results
