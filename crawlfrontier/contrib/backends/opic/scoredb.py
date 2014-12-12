"""
A simple association between pages and scores.
Score is in this a single real number.
"""
from abc import ABCMeta, abstractmethod

import sqlite


class ScoreDBInterface(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """Delete all contents"""
        pass

    @abstractmethod
    def add(self, page_id, page_score):
        """Add a new association"""
        pass

    @abstractmethod
    def get(self, page_id):
        """Get score for the given page"""
        pass

    @abstractmethod
    def set(self, page_id, page_score):
        """Change score"""
        pass

    @abstractmethod
    def delete(self, page_id):
        pass

    @abstractmethod
    def get_best_scores(self, n):
        """Get the n highest scores as a list of tuples of type
        (page_id, page_score)
        """
        pass

    @abstractmethod
    def iscores(self):
        """An iterator over all tuples (page_id, page_score)"""
        pass

    @abstractmethod
    def close(self):
        pass


class SQLite(sqlite.Connection, ScoreDBInterface):
    """A SQLite implementation for the ScoreDBInterface"""
    def __init__(self, db=None):
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS scores (
               page_id TEXT UNIQUE,
               score   REAL
            );

            CREATE INDEX IF NOT EXISTS
                score_index on scores(score);
            """
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM scores;
            """
        )

    def add(self, page_id, page_score):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO scores VALUES (?,?)
            """,
            (page_id, page_score)
        )

    def get(self, page_id):
        self._cursor.execute(
            """
            SELECT score FROM scores WHERE page_id=?
            """,
            (page_id,)
        )
        score = self._cursor.fetchone()
        if score:
            score = score[0]
        else:
            score = 0.0

        return score

    def set(self, page_id, page_score):
        self._cursor.execute(
            """
            UPDATE OR IGNORE scores
            SET score=?
            WHERE page_id=?
            """,
            (page_score, page_id)
        )

    def delete(self, page_id):
        self._cursor.execute(
            """
            DELETE FROM scores WHERE page_id=?
            """,
            (page_id,)
        )

    def get_best_scores(self, n):
        self._cursor.execute(
            """
            SELECT page_id, score FROM scores ORDER BY score DESC LIMIT ?
            """,
            (n,)
        )
        return self._cursor.fetchall()

    def iscores(self):
        return sqlite.CursorIterator(
            self._connection
                .cursor()
                .execute(
                    """
                    SELECT page_id, score FROM scores
                    """
                )
        )

    def close(self, comit=True):
        if not self._closed:
            if comit:
                self._connection.commit()
            self._connection.close()

        self._closed = True
