"""
Some utilities for SQLite 
"""
import sqlite3

class CursorIterator(object):
    """An iterator for a sqlite3.Cursor which closes 
    when the iteration ends
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.cursor.next()
        except StopIteration:
            self.cursor.close()
            raise

class Connection(object):
    def __init__(self, db=None):
        self._db = db or ':memory:'                
        self._batch = False

        self.open()

    def open(self):
        self._connection = sqlite3.connect(self._db)
        # auto-commit
        self._connection.isolation_level = None

        self._cursor = self._connection.cursor()        
        self._cursor.executescript(
            """
            PRAGMA cache_size=2000000;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=2;
            """
        )
        self._closed = False

    def close(self, comit=True):
        if not self._closed:
            if comit:
                self._connection.commit()
            self._connection.close()

        self._closed = True


    def start_batch(self):
        if not self._batch:
            self._batch = True
            self._cursor.execute('BEGIN;')


    def end_batch(self):
        if self._batch:
            self._batch = False
            self._cursor.execute('END;')
