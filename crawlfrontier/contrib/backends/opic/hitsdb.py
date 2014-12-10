"""
A HITS database associates a page identification (think page hash) to 
a HitsScore instance, containing all the hub/authority information 
necessary for the OPIC-HITS algorithm
"""
from abc import ABCMeta, abstractmethod
from itertools import imap

import sqlite

class HitsDBInterface(object):
    """Interface definition for every HITS database"""
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

    @abstractmethod
    def get_highest_h_cash(self, n=1):
        """
        Get the highest hub cash
        """
        pass

    @abstractmethod
    def get_highest_a_cash(self, n=1):
        """
        Get the highest authority cash
        """
        pass

    @abstractmethod
    def increase_all_cash(self, h_cash, a_cash):
        """
        Increase the cash in all pages in this amount
        """
        pass

    @abstractmethod
    def increase_h_cash(self, page_id_list, h_cash):
        """
        Increase the cash in the given pages in this amount
        """
        pass

    @abstractmethod
    def increase_a_cash(self, page_id_list, a_cash):
        """
        Increase the cash in the given pages in this amount
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
    
class SQLite(sqlite.Connection, HitsDBInterface):
    """SQLite based implementation for HitsDBInterface"""

    def __init__(self, db=None):
        """Make a new connection to a HITS scores database or, if 
        None provided make a new in-memory
        """
        super(SQLite, self).__init__(db)

        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS page_score (
                page_id   TEXT UNIQUE,
                h_history REAL,
                h_cash    REAL,
                a_history REAL,
                a_cash    REAL
            );

            CREATE INDEX IF NOT EXISTS
                h_cash_index on page_score(h_cash);

            CREATE INDEX IF NOT EXISTS
                a_cash_index on page_score(a_cash);


            CREATE TABLE IF NOT EXISTS global_increase (
                name  TEXT unique,
                value REAL
            );

            INSERT OR IGNORE INTO global_increase VALUES 
                ('a_cash', 0.0),
                ('h_cash', 0.0);

            """
        )
        self._a_cash_increase = self._restore_a_cash_increase()
        self._h_cash_increase = self._restore_h_cash_increase();


    def _restore_a_cash_increase(self):
        return self._cursor.execute(
            """
            SELECT value FROM global_increase WHERE name='a_cash'
            """
        ).fetchone()[0]

    def _restore_h_cash_increase(self):
        return self._cursor.execute(
            """
            SELECT value FROM global_increase WHERE name='h_cash'
            """
        ).fetchone()[0]

    def _save_a_cash_increase(self):
        return self._cursor.execute(
            """
            UPDATE global_increase SET value=? WHERE name='a_cash'
            """,
            (self._a_cash_increase,)
        )
    def _save_h_cash_increase(self):
        return self._cursor.execute(
            """
            UPDATE global_increase SET value=? WHERE name='h_cash'
            """,
            (self._h_cash_increase,)
        )

    def clear(self):
        self._cursor.executescript(
            """
            DELETE FROM page_score;
            """
        )

    def add(self, page_id, page_score):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO page_score VALUES (?,?,?,?,?)            
            """,
            (page_id,
             page_score.h_history,
             page_score.h_cash - self._h_cash_increase,
             page_score.a_history,
             page_score.a_cash - self._a_cash_increase)
        )
        
    def get(self, page_id):
        scores = self._cursor.execute(
            """
            SELECT h_history, h_cash, a_history, a_cash FROM page_score WHERE page_id=?
            """,
            (page_id,)
        ).fetchone()

        if scores:
            return HitsScore(scores[0], scores[1] + self._h_cash_increase,
                scores[2], scores[3] + self._a_cash_increase)
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
             page_score.h_cash - self._h_cash_increase,
             page_score.a_history,
             page_score.a_cash - self._a_cash_increase,
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

    def get_highest_h_cash(self, n=1):
        self._cursor.execute(
            """
            SELECT page_id, h_cash FROM page_score ORDER BY h_cash DESC LIMIT ?
            """,
            (n,)
        )
        return ((pid, c + self._h_cash_increase)
            for (pid, c) in self._cursor.fetchall())

    def get_highest_a_cash(self, n=1):
        self._cursor.execute(
            """
            SELECT page_id, a_cash FROM page_score ORDER BY a_cash DESC LIMIT ?
            """,
            (n,)
        )
        return ((pid, a + self._a_cash_increase)
            for (pid, a) in self._cursor.fetchall())

    def increase_all_cash(self, h_cash, a_cash):
        # TODO: we should be saving the deltas to the database (ideally
        #    periodically) and load on startup so this it will work when using
        #    a persistent db
        self._a_cash_increase += a_cash
        self._h_cash_increase += h_cash

        # eventually we may want to reset the db and write back the increases to
        # avoid precision loss (with reals) or overflow/underflow if we use int

    def increase_h_cash(self, page_id_list, h_cash):
        self._cursor.execute(
            """
            UPDATE OR IGNORE page_score 
            SET h_cash=h_cash + {0}
            WHERE page_id IN ({1})
            """.format(
                h_cash,
                ','.join(["'" + x + "'" for x in page_id_list])
            )
        )        

    def increase_a_cash(self, page_id_list, a_cash):
        self._cursor.execute(
            """
            UPDATE OR IGNORE page_score 
            SET a_cash=a_cash + {0}
            WHERE page_id IN ({1})
            """.format(
                a_cash,
                ','.join(["'" + x + "'" for x in page_id_list])
            )
        )        

    def close(self):
        self._save_a_cash_increase()
        self._save_h_cash_increase()

        super(SQLite, self).close()
