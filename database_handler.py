import sqlite3 as lite
import uuid
from sqlite3 import Error

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from setup import Config


class DataBaseHandler():
    def __init__(self, config_path: str = "config.yml", config_dict: dict = None,
                 create_all: bool = True):
        """Initializes class by either connecting to an existing database
        or by creating a new database. Database settings depend on config.yml

        Args:
            config_file (str): Path to configuration file. Defaults to "config.yml"
            config_dict (dict): Dictionary containing the config information (in case
                                the dictionary shall be directly passed instead of read
                                out of a configuration file).
            create_all (bool): If set to false, will not attempt to create the friends,
                               result, and user_details tables.
        Returns:
            Nothing
        """

        self.config = Config(config_path, config_dict)
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
                self.engine = lite.connect(self.config.dbname + ".db")
                print("Connected to " + self.config.dbname + "!")
            except Error as e:
                raise e
            if create_all:
                try:
                    create_friends_table_sql = """CREATE TABLE IF NOT EXISTS friends (
                                                    source BIGINT NOT NULL,
                                                    target BIGINT NOT NULL,
                                                    burned TINYINT NOT NULL,
                                                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                                  );"""
                    create_friends_index_sql_1 = "CREATE INDEX iFSource ON friends(source);"
                    create_friends_index_sql_2 = "CREATE INDEX iFTimestamp ON friends(timestamp);"
                    create_results_table_sql = """CREATE TABLE IF NOT EXISTS result (
                                                    source BIGINT NOT NULL,
                                                    target BIGINT NOT NULL,
                                                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                                  );"""
                    create_results_index_sql_1 = "CREATE INDEX iRSource ON result(source);"
                    create_results_index_sql_2 = "CREATE INDEX iRTimestamp ON result(timestamp);"
                    c = self.engine.cursor()
                    c.execute(create_friends_table_sql)
                    c.execute(create_friends_index_sql_1)
                    c.execute(create_friends_index_sql_2)
                    c.execute(create_results_table_sql)
                    c.execute(create_results_index_sql_1)
                    c.execute(create_results_index_sql_2)
                    if user_details_list != []:
                        create_user_details_sql = """
                            CREATE TABLE IF NOT EXISTS user_details
                            (""" + ", ".join(user_details_list) + """,
                             timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);"""
                        create_ud_index = "CREATE INDEX iUTimestamp ON user_details(timestamp)"
                        c.execute(create_user_details_sql)
                        c.execute(create_ud_index)
                    else:
                        # TODO: Make this a minimal user_details table?
                        print("""No user_details configured in config.yml. Will not create a
                              user_details table.""")
                except Error as e:
                    print(e)

        elif self.config.dbtype.lower() == "mysql":
            try:
                self.engine = create_engine(
                    f'mysql+pymysql://{self.config.dbuser}:'
                    f'{self.config.dbpwd}@{self.config.dbhost}/{self.config.dbname}'
                )
                print('Connected to database "' + self.config.dbname + '" via mySQL!')
            except OperationalError as e:
                raise e
            if create_all:
                try:
                    create_friends_table_sql = """CREATE TABLE IF NOT EXISTS friends (
                                                    source BIGINT NOT NULL,
                                                    target BIGINT NOT NULL,
                                                    burned TINYINT NOT NULL,
                                                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                                    ON UPDATE CURRENT_TIMESTAMP,
                                                    UNIQUE INDEX fedge (source, target),
                                                    INDEX(timestamp)
                                                    );"""
                    create_results_table_sql = """CREATE TABLE IF NOT EXISTS result (
                                                    source BIGINT NOT NULL,
                                                    target BIGINT NOT NULL,
                                                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                    UNIQUE INDEX redge (source, target),
                                                    INDEX(timestamp)
                                                  );"""
                    self.engine.execute(create_friends_table_sql)
                    self.engine.execute(create_results_table_sql)
                    if user_details_list != []:
                        create_user_details_sql = """
                            CREATE TABLE IF NOT EXISTS user_details
                            (""" + ", ".join(user_details_list) + """, timestamp TIMESTAMP
                            DEFAULT CURRENT_TIMESTAMP,
                            INDEX(timestamp));"""
                        self.engine.execute(create_user_details_sql)
                    else:
                        print("""No user_details configured in config.yml. Will not create a
                              user_details table.""")
                except OperationalError as e:
                    raise e

    def make_temp_tbl(self, type: str = "user_details"):
        """Creates a new temporary table with a random name consisting of a temp_ prefix
           and a uid. The structure of the table depends on the chosen type param. The
           table's structure will be a copy of an existing table, for example, a temporary
           user_details table will have the same columns and attributes (Keys, constraints, etc.)
           as the user_details table.

        Args:
            type (str): The table that the temporary table is going to simulate.
                        Possible values are ["friends", "result", "user_details"]
        Returns:
            The name of the temporary table.
        """
        uid = uuid.uuid4()
        temp_tbl_name = "temp_" + str(uid).replace('-', '_')

        if self.config.dbtype.lower() == "mysql":
            create_temp_tbl_sql = f"CREATE TABLE {temp_tbl_name} LIKE {type};"
        elif self.config.dbtype.lower() == "sqlite":
            create_temp_tbl_sql = f"CREATE TABLE {temp_tbl_name} AS SELECT * FROM {type} WHERE 0"
        self.engine.execute(create_temp_tbl_sql)
        return temp_tbl_name

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
        temp_tbl_name = self.make_temp_tbl(type="friends")

        friends_df = pd.DataFrame({'target': friendlist})
        friends_df['source'] = seed
        friends_df['burned'] = 0
        friends_df.to_sql(name=temp_tbl_name, con=self.engine, if_exists="replace", index=False)

        if self.config.dbtype.lower() == "mysql":
            insert_query = f"""
                INSERT INTO friends (source, target, burned)
                SELECT source, target, burned
                FROM {temp_tbl_name}
                ON DUPLICATE KEY UPDATE
                    source = {temp_tbl_name}.source
            """
        elif self.config.dbtype.lower() == "sqlite":
            insert_query = f"""
                INSERT OR IGNORE INTO friends (source, target, burned)
                SELECT source, target, burned
                FROM {temp_tbl_name}
            """

        self.engine.execute(insert_query)
        self.engine.execute(f"DROP TABLE {temp_tbl_name}")
