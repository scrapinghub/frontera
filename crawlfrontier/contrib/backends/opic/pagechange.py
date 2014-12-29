"""
Interface definition for web page change detection
"""
from abc import ABCMeta, abstractmethod
from hashlib import sha1

import hashdb


class Status(object):
    EQUAL = 0
    UPDATED = 1
    NEW = 2


class PageChangeInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update(self, page_id, page_body):
        """Returns one of the change status for the page

        The value returned is one of the fields of Status
        """
        pass


class BodySHA1(PageChangeInterface):
    def __init__(self, db=None):
        self._db = db or hashdb.SQLite()

    def update(self, page_id, page_body):
        new_hash = sha1(page_body).hexdigest()
        old_hash = self._db.get(page_id)
        if not old_hash:
            change = Status.NEW
            self._db.add(page_id, new_hash)
        else:
            self._db.set(page_id, new_hash)
            if new_hash != old_hash:
                change = Status.UPDATED
            else:
                change = Status.EQUAL

        return change
