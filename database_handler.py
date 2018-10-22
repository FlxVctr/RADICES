import sqlite3 as lite
from sqlite3 import Error
from setup import Config


class DataBaseHandler():

    def __init__(self):
        # TODO: create database connection if dbtype = SQL
        config = Config()
        if config.dbtype == "sqlite":
            try:
                conn = lite.connect(config.dbname + ".db")
            except Error as e:
                print(e)
            finally:
                conn.close()

    def new_db(self, db_name):
        """Creates a new sqlite database

        Args:
            db_name (str, optional): Name for the database

        Returns:
            Nothing
        """
        try:
            conn = lite.connect(db_name + ".db")
        except Error as e:
            print(e)
        finally:
            conn.close()
