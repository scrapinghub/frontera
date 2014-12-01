"""
Database for PageData
"""
from abc import ABCMeta, abstractmethod
import sqlite3

class PageDBInterface(object):
    """Interface for the Page database"""
    __metaclass__ = ABCMeta

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

class PageData(object):
    """A container for the necessary page data"""
    def __init__(self, url, domain):
        self.url = url
        self.domain = domain

class SQLite(PageDBInterface):
    """SQLite implementation of the page database interface"""
    def __init__(self, db=None):
        if not db:
            db = ':memory:'

        self._connection = sqlite3.connect(db)
        self._cursor = self._connection.cursor()
        

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS pages (
               page_id TEXT UNIQUE,
               url     TEXT,
               domain  TEXT
            );

            CREATE INDEX IF NOT EXISTS
                page_id_index on pages(page_id);
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
