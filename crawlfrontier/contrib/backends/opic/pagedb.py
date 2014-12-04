"""
Database for PageData
"""
from abc import ABCMeta, abstractmethod
import sqlite

class PageDBInterface(object):
    """Interface for the Page database"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """Delete all contents"""
        pass

    @abstractmethod
    def add(self, page_id, page):
        """Associate page_id to PageData"""
        pass

    @abstractmethod
    def get(self, page_id):
        """Get associated PageData to page_id"""
        pass

    @abstractmethod
    def set(self, page_id, page_data):
        """Change associated PageData"""
        pass

    @abstractmethod
    def delete(self, page_id):
        """Delete association"""
        pass

    @abstractmethod
    def close(self):
        """Close database"""
        pass

class PageData(object):
    """A container for the necessary page data"""
    def __init__(self, url, domain):
        self.url = url
        self.domain = domain

class SQLite(sqlite.Connection, PageDBInterface):
    """SQLite implementation of the page database interface"""
    def __init__(self, db=None):
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS pages (
               page_id TEXT UNIQUE,
               url     TEXT,
               domain  TEXT
            );
            """
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM pages;
            """
        )

    def add(self, page_id, page):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO pages VALUES (?,?,?)            
            """,
            (page_id, page.url, page.domain)
        )

    def get(self, page_id):
        page_data = self._cursor.execute(
            """
            SELECT url, domain FROM pages WHERE page_id=?
            """,
            (page_id,)
        ).fetchone()

        if page_data:
            return PageData(*page_data)
        else:
            return None

    def set(self, page_id, page_data):
        self._cursor.execute(
            """
            UPDATE OR IGNORE pages
            SET url=?, domain=?
            WHERE page_id=?                
            """,
            (page_data.url, page_data.domain, page_id)                 
        )

    def delete(self, page_id):
        self._cursor.execute(
            """
            DELETE FROM pages WHERE page_id=?
            """,
            (page_id,)
        )

