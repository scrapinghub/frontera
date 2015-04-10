"""
This is intended as an extremely simple, but useful, backend.
It has two nice properties:
    - You can restart the crawl and it will continue where it left last time
    - It will not revisit (if possible) already seen links. Actually, what it
      really does is give priority to the less crawled links.
"""
import sqlite3

from crawlfrontier import Backend
from crawlfrontier.core.models import Request


class LinksDB(object):
    """An SQLite database to remember which pages have already been crawled"""

    def __init__(self, db=None):
        self._connection = sqlite3.connect(db)
        self._connection.isolation_level = None
        self._cursor = self._connection.cursor()

        # Some performance options
        self._cursor.executescript(
            """
            PRAGMA cache_size=2000000;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=2;
            """
        )

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS links (
               page_id  TEXT UNIQUE,
               url      TEXT,
               requests INTEGER
            );

            CREATE INDEX IF NOT EXISTS
                req_index on links(requests);
            """
        )

    def close(self, comit=True):
        self._connection.commit()
        self._connection.close()

    def clear(self):
        self._cursor.executescript("DELETE FROM links;")

    def add(self, page_id, url):
        self._cursor.execute(
            "INSERT OR IGNORE INTO links VALUES (?,?,?)",
            (page_id, url, 0)
        )

    def next(self, n_links):
        pages = self._cursor.execute(
            "SELECT page_id, url FROM links ORDER BY requests ASC LIMIT ?",
            (n_links,)
        ).fetchall()

        urls = []
        if pages:
            self._cursor.execute(
                """
                UPDATE OR IGNORE links SET requests = requests + 1
                WHERE page_id in ({0})
                """.format(
                    ','.join(["'" + p[0] + "'" for p in pages]))
            )
            urls =  [p[1] for p in pages]
        return urls


class NoRevisit(Backend):
    def __init__(self, db=None):
        """If not database is provided it will create one in-memory"""
        self._db = LinksDB(db or ':memory:')

    @classmethod
    def from_manager(cls, manager):
        return cls(manager.settings.get('NOREVISIT_DB_PATH', None))

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self._db.close()

    def add_seeds(self, seeds):
        for link in seeds:
            self._db.add(link.meta['fingerprint'], link.url)

    def request_error(self, page, error):
        pass

    def page_crawled(self, response, links):
        self.add_seeds(links)

    def get_next_requests(self, max_n_requests):
        return map(Request, self._db.next(max_n_requests))
