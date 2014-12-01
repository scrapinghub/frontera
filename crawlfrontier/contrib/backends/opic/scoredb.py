"""
A simple association between pages and scores.
Score is in this a single real number.
"""
from abc import ABCMeta, abstractmethod
import sqlite3

from sqlite import CursorIterator

class ScoreDBInterface(object):
    __metaclass__ = ABCMeta

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

class SQLite(ScoreDBInterface):
    """A SQLite implementation for the ScoreDBInterface"""
    def __init__(self, db=None):
        if not db:
            db = ':memory:'

        self._connection = sqlite3.connect(db)
        self._cursor = self._connection.cursor()
        

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS scores (
               page_id TEXT UNIQUE,
               score   REAL
            );

            CREATE INDEX IF NOT EXISTS
                page_id_index on scores(page_id);

            CREATE INDEX IF NOT EXISTS
                score_index on scores(score);
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
        return CursorIterator(
            self._connection
                .cursor()
                .execute(
                    """
                    SELECT page_id, score FROM scores
                    """
                )
        )        
