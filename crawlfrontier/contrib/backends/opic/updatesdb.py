"""
Store for each page the necessary state parameters for maintaining an estimate
of the refresh frequency
"""
from abc import ABCMeta, abstractmethod

import sqlite


class UpdatesDBInterface(object):
    """Interface definition for every FreqEstDB database"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """Delete all contents"""
        pass

    @abstractmethod
    def close(self):
        """Close connection and commit changes """
        pass

    @abstractmethod
    def add(self, page_id, start_time, updates):
        """Add new entry for page_id where:

        page_id    -- An string which identifies the page
        start_time -- Time in seconds since epoch
        updates    -- How many updates
        """
        pass

    @abstractmethod
    def increment(self, page_id, updates):
        """Increment number of updates"""
        pass

    @abstractmethod
    def get(self, page_id):
        """
        Get start_time and updates for page_id
        """
        pass

    @abstractmethod
    def delete(self, page_id):
        """Delete association"""
        pass


class SQLite(sqlite.Connection, UpdatesDBInterface):
    """SQLite based implementation for FreqEstDBInterface"""

    def __init__(self, db=None):
        """Make a new connection to a frequency estimation database or, if
        None provided make a new in-memory
        """
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS page_updates (
                page_id    TEXT UNIQUE,
                start_time REAL,
                updates    INTEGER
            );
            """
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM page_updates;
            """
        )

    def add(self, page_id, start_time, updates):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO page_updates VALUES (?, ?, ?)
            """,
            (page_id,
             start_time,
             updates)
        )

    def increment(self, page_id, updates):
        self._cursor.execute(
            """
            UPDATE OR IGNORE page_updates
            SET updates=updates+?
            WHERE page_id=?
            """,
            (updates, page_id)
        )

    def get(self, page_id):
        return self._cursor.execute(
            """
            SELECT start_time, updates FROM page_updates
            WHERE page_id=?
            """,
            page_id
        ).fetchone()

    def delete(self, page_id):
        self._cursor.execute(
            """
            DELETE FROM page_updates WHERE page_id=?
            """,
            (page_id,)
        )

    def __contains__(self, page_id):
        return self.get(page_id) is not None
