"""
Interface definition for graphs and a simple SQLite based implementation
"""
from abc import ABCMeta, abstractmethod
from itertools import imap
import sqlite3

from sqlite import CursorIterator

class GraphInterface(object):
    """Interface definition for a Graph database"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def close(self):
        """Close connection and commit changes"""
        pass

    @abstractmethod
    def add_node(self, node):
        """Add a new node"""
        pass

    @abstractmethod
    def has_node(self, node):
        """True if node present inside graph"""
        pass

    @abstractmethod
    def delete_node(self, node):
        """Delete node, and all edges connecting to this node"""
        pass

    @abstractmethod
    def add_edge(self, start, end):
        """Add a new edge to the graph, from start to end"""
        pass

    @abstractmethod
    def delete_edge(self, node):
        """Delete edge"""
        pass

    @abstractmethod
    def successors(self, node):
        """A list of the successors for the given node"""
        pass

    @abstractmethod
    def predecessors(self, node):
        """A list of the predecessors for the given node"""
        pass

    @abstractmethod
    def inodes(self):
        """An iterator for all the nodes"""
        pass

    @abstractmethod
    def iedges(self):
        """An iterator for all the edges"""
        pass
    

class SQLite(GraphInterface):
    """SQLite implementation of GraphInterface"""
    def __init__(self, db=None):
        if not db:
            db = ':memory:'

        self._connection = sqlite3.connect(db)
        # auto-commit
        self._connection.isolation_level = None

        self._cursor = self._connection.cursor()
      
        self._cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS edges (
               start TEXT NOT NULL,
               end   TEXT NOT NULL,

               PRIMARY KEY (start, end)
            );

            CREATE TABLE IF NOT EXISTS nodes (
               name TEXT UNIQUE
            );

            CREATE INDEX IF NOT EXISTS
                node_index on nodes(name);

            CREATE INDEX IF NOT EXISTS
                start_index on edges(start);

            CREATE INDEX IF NOT EXISTS
                end_index on edges(end);
            """
        )

    def add_node(self, node):
        self._cursor.execute(
            """
            INSERT OR IGNORE INTO nodes VALUES (?)            
            """,
            (node,)
        )

    def has_node(self, node):
        self._cursor.execute(
            """
            SELECT name FROM nodes WHERE name=? LIMIT 1
            """,
            (node,)
        )
        
        return self._cursor.fetchone() != None

    def delete_node(self, node):
        self._cursor.execute(
            """
            DELETE FROM nodes WHERE name=?
            """,
            (node,)
        )
        self._cursor.execute(
            """
            DELETE FROM edges WHERE start=? OR end=?
            """,
            (node, node)
        )

    def add_edge(self, start, end):        
        self.add_node(start)
        self.add_node(end)

        self._cursor.execute(
            """
            INSERT OR IGNORE INTO edges(start, end) VALUES (?, ?)
            """,
            (start, end)
        )

    def delete_edge(self, start, end):
        self._cursor.execute(
            """
            DELETE FROM edges WHERE start=? AND end=?
            """,
            (start, end)
        )

    def successors(self, node):
        self._cursor.execute(
            """
            SELECT end FROM edges WHERE start=?
            """,
            (node,)
        )
        return map(lambda x: x[0], # un-tuple
                   self._cursor.fetchall())

    def predecessors(self, node):
        self._cursor.execute(
            """
            SELECT start FROM edges WHERE end=?
            """,
            (node,)
        )
        return map(lambda x: x[0], # un-tuple
                   self._cursor.fetchall())



    def inodes(self):        
        return imap(lambda x: x[0], # un-tuple
                    CursorIterator(
                        self._connection
                            .cursor()
                            .execute('SELECT name FROM nodes')
                    )
        )

    def iedges(self):
        return CursorIterator(
            self._connection
                .cursor()
                .execute('SELECT start,end FROM edges')
        )

    def close(self, comit=True):
        self._connection.commit()
        self._connection.close()
