"""
Some utilities for SQLite 
"""

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
