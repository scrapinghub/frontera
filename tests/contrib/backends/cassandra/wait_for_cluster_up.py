from time import time

from cassandra.cluster import Cluster, NoHostAvailable

_cluster_ips = ['127.0.0.1']
_port = 9042
_timeout = 36000  # 10 minutes


def _is_cluster_up():
    cluster = Cluster(_cluster_ips, _port)
    try:
        cluster.connect()
        return True
    except NoHostAvailable:
        return False


def _wait_for_cluster_to_start():
    print('waiting for cassandra cluster to setup...')
    start = time()
    while not _is_cluster_up():
        time_taken = time() - start
        if time_taken > _timeout:
            raise TimeoutError('Cassandra node could not start within the timeout.')
    time_taken = time() - start
    print('cassandra cluster is up! Waited for %s seconds.' % int(time_taken))

if __name__ == '__main__':
    _wait_for_cluster_to_start()
