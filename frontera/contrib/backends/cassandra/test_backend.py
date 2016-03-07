import os

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from frontera.tests import backends
from frontera.tests.test_revisiting_backend import RevisitingBackendTest


#----------------------------------------------------
# SQAlchemy base classes
#----------------------------------------------------
class cassandraFIFO(backends.FIFOBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.FIFO'


class cassandraLIFO(backends.LIFOBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.LIFO'


class cassandraDFS(backends.DFSBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.DFS'


class cassandraBFS(backends.BFSBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.BFS'


class cassandraRevisiting(RevisitingBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.revisiting.Backend'

