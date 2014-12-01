"""
A HITS database associates a page identification (think page hash) to 
a HitsScore instance, containing all the hub/authority information 
necessary for the OPIC-HITS algorithm
"""
from abc import ABCMeta, abstractmethod
from itertools import imap
import sqlite3

class HitsDBInterface(object):
    """Interface definition for every HITS database"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def close(self):
        """Close connection and commit changes """
        pass

    @abstractmethod
    def add(self, page_id, page_score):
        """Associate page_score with page_id, where:

        page_id    -- An string which identifies the page 
        page_score -- An instance of HitsScore
        """
        pass

    @abstractmethod
    def get(self, page_id):
        """Get the HitsScore associated with page_id"""
        pass

    @abstractmethod
    def set(self, page_id, page_score):
        """Change the HitsScore associated with page_id"""
        pass

    @abstractmethod
    def delete(self, page_id):
        """Delete association"""
        pass

    @abstractmethod
    def __contains__(self, page_id):
        """True if an association exists for page_id"""
        pass

    @abstractmethod
    def iteritems(self):
        """Return an iterator over the tuples (page_id, page_score), 
        where:
         
            -page_id    -- An string identifier for the page
            -page_score -- A HitsScore instance
        """
        pass

class HitsScore(object):
    """Just a container for the following (modifiable) fields:

       -h_history -- Accumulated hub cash
       -h_cash    -- Non-spent hub cash
       -a_history -- Accumulated authority cash
       -a_cash    -- Non-spent authority cash
    """
    def __init__(self, h_history, h_cash, a_history, a_cash):
        self.h_history = h_history
        self.h_cash = h_cash
        self.a_history = a_history
        self.a_cash = a_cash
    
class SQLite(HitsDBInterface):
    """SQLite based implementation for HitsDBInterface"""

    def __init__(self, db=None):
        """Make a new connection to a HITS scores database or, if 
        None provided make a new in-memory
        """
        if not db:
            db = ':memory:'

        self._connection = sqlite3.connect(db)
        # auto-commit 
        self._connection.isolation_level = None 

        self._cursor = self._connection.cursor()

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS page_score (
                page_id   TEXT,
                h_history REAL,
                h_cash    REAL,
                a_history REAL,
                a_cash    REAL
            );

            CREATE INDEX IF NOT EXISTS
                page_id_index on page_score(page_id);

            CREATE INDEX IF NOT EXISTS
                h_cash_index on page_score(h_cash);

            CREATE INDEX IF NOT EXISTS
                a_cash_index on page_score(a_cash);
            """
        )
      
    def close(self):
        self._connection.commit()
        self._connection.close()

    def add(self, page_id, page_score):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO page_score VALUES (?,?,?,?,?)            
            """,
            (page_id,
             page_score.h_history,
             page_score.h_cash,
             page_score.a_history,
             page_score.a_cash)
        )
        
    def get(self, page_id):
        scores = self._cursor.execute(
            """
            SELECT h_history, h_cash, a_history, a_cash FROM page_score WHERE page_id=?
            """,
            (page_id,)
        ).fetchone()

        if scores:
            return HitsScore(*scores)
        else:
            return None
        

    def set(self, page_id, page_score):
        self._cursor.execute(
            """
            UPDATE OR IGNORE page_score 
            SET h_history=?, h_cash=?, a_history=?, a_cash=? 
            WHERE page_id=?
            """,
            (page_score.h_history,
             page_score.h_cash,
             page_score.a_history,
             page_score.a_cash,
             page_id)
        )

    def delete(self, page_id):
        self._cursor.execute(
            """
            DELETE FROM page_score WHERE page_id=?
            """,
            (page_id,)
        )

    def __contains__(self, page_id):
        self._cursor.execute(
            """
            SELECT page_id FROM page_score WHERE page_id=? LIMIT 1
            """,
            (page_id,)
        )
        
        return self._cursor.fetchone() != None        


    def iteritems(self):
        return imap(
            lambda x: (x[0], HitsScore(*x[1:])),
            self._cursor.execute(
                """
                SELECT * FROM page_score
                """
            )
        )
