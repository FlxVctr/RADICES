import sqlite3 as lite
import pandas as pd
from sqlite3 import Error
from setup import Config
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


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
        user_details_list = []
        if "twitter_user_details" in self.config.config:
            for detail, sqldatatype in self.config.config["twitter_user_details"].items():
                if sqldatatype is not None:
                    user_details_list.append(detail + " " + sqldatatype)
        else:
            print("""Key "twitter_user_details" could not be found in config.yml. Will not create
                  a user_details table.""")

        if self.config.dbtype.lower() == "sqlite":
            try:
                self.conn = lite.connect(self.config.dbname + ".db")
                print("Connected to " + self.config.dbname + "!")
            except Error as e:
                raise e
            # TODO: timestamp für friends table?
            try:
                create_friends_table_sql = """ CREATE TABLE IF NOT EXISTS friends (
                                            id integer PRIMARY KEY,
                                            source text NOT NULL,
                                            target text NOT NULL,
                                            burned tinyint NOT NULL
                                            ); """
                c = self.conn.cursor()
                c.execute(create_friends_table_sql)
                if user_details_list != []:
                    create_user_details_sql = """
                        CREATE TABLE IF NOT EXISTS user_details
                        (""" + ", ".join(user_details_list) + ");"
                    c.execute(create_user_details_sql)
                else:
                    print("""No user_details configured in config.yml. Will not create a
                          user_details table.""")
            except Error as e:
                print(e)

        elif self.config.dbtype.lower() == "mysql":
            try:
                self.engine = create_engine(
                    'mysql+pymysql://' + self.config.dbuser + ':' + self.config.dbpwd + '@' +
                    self.config.dbhost + '/' + self.config.dbname)
                print('Connected to database "' + self.config.dbname + '" via mySQL!')
            except OperationalError as e:
                raise e
            try:
                create_friends_table_sql = """CREATE TABLE IF NOT EXISTS friends (
                                             id MEDIUMINT NOT NULL AUTO_INCREMENT,
                                             source CHAR(30) NOT NULL,
                                             target CHAR(30) NOT NULL,
                                             burned TINYINT NOT NULL,
                                             PRIMARY KEY (id)
                                            );"""
                self.engine.execute(create_friends_table_sql)
                if user_details_list != []:
                    create_user_details_sql = """
                        CREATE TABLE IF NOT EXISTS user_details
                        (""" + ", ".join(user_details_list) + ");"
                    self.engine.execute(create_user_details_sql)
                else:
                    print("""No user_details configured in config.yml. Will not create a
                          user_details table.""")
            except OperationalError as e:
                raise e

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

        friends_df = pd.DataFrame({'target': friendlist})
        friends_df['source'] = seed
        friends_df['burned'] = 0
        friends_df.to_sql(name="friends", con=self.engine, if_exists="append", index=False)
