# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .common import *

BACKEND = 'frontera.contrib.backends.hbase.HBaseBackend'
HBASE_DROP_ALL_TABLES = True

MAX_NEXT_REQUESTS = 2048
NEW_BATCH_DELAY = 3.0