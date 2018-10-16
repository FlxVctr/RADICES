import sqlite3 as lite
from sqlite3 import Error
import pandas as pd


class DataBaseHandler():

    def new_db(self, db_name):
        '''
        Create a new SQLite database
        '''
        try:
            conn = lite.connect(db_name + ".db")
        except Error as e:
            print(e)
        finally:
            conn.close()
