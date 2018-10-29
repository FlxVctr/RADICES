import sqlite3 as lite
import pandas as pd
from sqlite3 import Error
from setup import Config


class DataBaseHandler():
    def __init__(self):
        """Initializes class by either connecting to an existing database
        or by creating a new database. Database settings depend on config.yml

        Args:
            None
        Returns:
            Nothing
        """
        # TODO: create database connection if dbtype = SQL
        self.config = Config()
        if self.config.dbtype == "sqlite":
            try:
                self.conn = lite.connect(self.config.dbname + ".db")
                print("Connected to " + self.config.dbname + "!")
            except Error as e:
                print(e)

            # TODO: timestamp für friends table?
            create_friends_table = """ CREATE TABLE IF NOT EXISTS friends (
                                        id integer PRIMARY KEY,
                                        user text NOT NULL,
                                        friend text NOT NULL,
                                        burned integer NOT NULL
                                    ); """
            try:
                c = self.conn.cursor()
                c.execute(create_friends_table)
            except Error as e:
                print(e)

    def write_friends(self, seed, friendlist):
        """Writes the database entries for one user and their friends in format user, friends.
        Note that the database is appended by the new entries, and that no entries will be deleted
        by this method.

        Args:
            seed (str): single Twitter ID
            friendlist (list of str): Twitter IDs of seed's friends
        Returns:
            Nothing
        """

        friends_df = pd.DataFrame({'friend': friendlist})
        friends_df['user'] = seed
        friends_df['burned'] = 0
        friends_df.to_sql(name="friends", con=self.conn, if_exists="append", index=False)
