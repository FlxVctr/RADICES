import sqlite3 as lite
from sqlite3 import Error


class DataBaseHandler():
    """Handles the interactions with the database."""

    def new_db(self, db_name):
        """Create a new SQLite database."""
        try:
            conn = lite.connect(db_name + ".db")
        except Error as e:
            print(e)
        finally:
            conn.close()
