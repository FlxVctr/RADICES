import argparse
import copy
import json
import multiprocessing.dummy as mp
import os
import queue
import sqlite3 as lite
import sys
import time
import unittest
import warnings
from json import JSONDecodeError
from sys import stdout

import numpy as np
import pandas as pd
import tweepy
import yaml
from datetime import datetime
from pandas.api.types import is_string_dtype
from pandas.errors import EmptyDataError
from pandas.io.sql import DatabaseError
from pandas.util.testing import assert_frame_equal, assert_index_equal
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError

# Needed so that developers do not have to append PYTHONPATH manually.
sys.path.insert(0, os.getcwd())

import helpers
import passwords
import test_helpers
from collector import Collector, Connection, Coordinator, retry_x_times, get_latest_tweets
from database_handler import DataBaseHandler
from exceptions import TestException
from setup import Config, FileImport
from start import main_loop

parser = argparse.ArgumentParser(description='SparseTwitter TestSuite')
parser.add_argument('-s', '--skip_draining_tests',
                    help='If set, module skips api call draining tests.',
                    required=False,
                    action='store_true')
parser.add_argument('-w', '--show_resource_warnings',
                    help='If set, will show possible resource warnings from the requests package.',
                    required=False,
                    action='store_true')
parser.add_argument('unittest_args', nargs='*')

args = parser.parse_args()
skiptest = args.skip_draining_tests
show_warnings = args.show_resource_warnings
sys.argv[1:] = args.unittest_args


def skipIfDraining():
    if skiptest:
        return unittest.skip("This test drains API calls")
    return lambda x: x


def setUpModule():
    if not show_warnings:
        warnings.filterwarnings(action="ignore",
                                message="unclosed",
                                category=ResourceWarning)
    if os.path.isfile("latest_seeds.csv"):
        os.rename("latest_seeds.csv",
                  "{}_latest_seeds.csv".format(datetime.now().isoformat().replace(":", "-")))


class FileImportTest(unittest.TestCase):

    def setUp(self):
        if os.path.isfile("keys.json"):
            os.rename("keys.json", "keys.json.bak")
        if os.path.isfile("seeds.csv"):
            os.rename("seeds.csv", "seeds.csv.bak")

    def tearDown(self):
        if os.path.isfile("keys.json.bak"):
            os.replace("keys.json.bak", "keys.json")
        if os.path.isfile("seeds.csv.bak"):
            os.replace("seeds.csv.bak", "seeds.csv")

    # Note that the test fails if start.py passes those tests.
    def test_read_app_key_file(self):
        # File not found
        with self.assertRaises(FileNotFoundError):
            FileImport().read_app_key_file()

        # File empty
        open("keys.json", 'a').close()
        with self.assertRaises(JSONDecodeError):
            FileImport().read_app_key_file()

        # File lacks required keys
        key_dict = {
            "consumer_toke": "abc",
            "consumer_secret": "xxx"
        }
        with open("keys.json", "w") as f:
            json.dump(key_dict, f)
        with self.assertRaises(KeyError):
            FileImport().read_app_key_file()
        key_dict = {
            "consumer_token": "abc",
            "consumer_secre": "xxx"
        }
        with open("keys.json", "w") as f:
            json.dump(key_dict, f)
        with self.assertRaises(KeyError):
            FileImport().read_app_key_file()

        # Wrong data types
        key_dict = {
            "consumer_token": 2,
            "consumer_secret": [1234]
        }
        with open("keys.json", "w") as f:
            json.dump(key_dict, f)
        with self.assertRaises(TypeError):
            FileImport().read_app_key_file()

        # Return is a tuple of strings
        if os.path.isfile("keys.json.bak"):
            os.replace("keys.json.bak", "keys.json")
        self.assertIsInstance(FileImport().read_app_key_file(), tuple)
        self.assertIsInstance(FileImport().read_app_key_file()[0], str)
        self.assertIsInstance(FileImport().read_app_key_file()[1], str)

    def test_read_seed_file(self):
        # File is missing
        with self.assertRaises(FileNotFoundError):
            FileImport().read_seed_file()

        # File is empty
        open("seeds.csv", 'a').close()
        with self.assertRaises(EmptyDataError):
            FileImport().read_seed_file()

    # TODO: Check DataType of ID column of seeds.csv

    def test_read_token_file_raises_error_if_no_file(self):
        with self.assertRaises(FileNotFoundError):
            FileImport().read_token_file(filename='no_file.csv')

    def test_outputs_df_with_two_string_columns(self):

        tokens = FileImport().read_token_file()

        self.assertIsInstance(tokens, pd.DataFrame)

        token_dtype = is_string_dtype(tokens['token'])
        secret_dtype = is_string_dtype(tokens['secret'])

        self.assertTrue(token_dtype)
        self.assertTrue(secret_dtype)

        self.assertIsInstance(tokens['token'][0], str)
        self.assertIsInstance(tokens['secret'][0], str)


class DataBaseHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.isfile("config.yml"):
            os.replace("config.yml", "config.yml.bak")

    @classmethod
    def tearDownClass(cls):
        if os.path.isfile("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")

    def tearDown(self):
        dbh = DataBaseHandler(config_dict=test_helpers.config_dict_user_details_dtypes_mysql,
                              create_all=False)
        if os.path.isfile(self.db_name + ".db"):
            os.remove(self.db_name + ".db")
        try:
            dbh.engine.execute("DROP TABLES friends;")
        except Exception:
            pass
        try:
            dbh.engine.execute("DROP TABLES result;")
        except Exception:
            pass
        try:
            dbh.engine.execute("DROP TABLES user_details;")
        except Exception:
            pass

    config_dict_mysql = test_helpers.config_dict_mysql
    config_dict_sqlite = test_helpers.config_dict_sqlite
    db_name = Config(config_dict=config_dict_sqlite).dbname

    def test_setup_and_dbh_creates_database_from_given_name(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.config_dict_sqlite, f, default_flow_style=False)

        DataBaseHandler()
        db_name = self.db_name
        try:
            self.assertTrue(os.path.isfile(db_name + ".db"))
        except AssertionError:
            print("Database was not created!")

    def test_dbh_creates_friends_and_results_tables_with_correct_columns(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.config_dict_sqlite, f, default_flow_style=False)
        DataBaseHandler()
        db_name = self.db_name
        conn = lite.connect(db_name + ".db")
        sql_out = pd.read_sql(con=conn, sql="SELECT * FROM friends").columns
        self.assertIn("target", sql_out)
        self.assertIn("source", sql_out)
        self.assertIn("burned", sql_out)
        self.assertIn("timestamp", sql_out)

        sql_out = pd.read_sql(con=conn, sql="SELECT * FROM result").columns
        self.assertIn("target", sql_out)
        self.assertIn("source", sql_out)
        self.assertIn("timestamp", sql_out)
        conn.close()

    @skipIfDraining()
    def test_dbh_write_friends_function_takes_input_and_writes_to_table(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.config_dict_sqlite, f, default_flow_style=False)
        seed = int(FileImport().read_seed_file().iloc[0])
        dbh = DataBaseHandler()
        c = Collector(Connection(), seed)
        friendlist = c.get_friend_list()
        dbh.write_friends(seed, friendlist)

        s = "SELECT target FROM friends WHERE source LIKE '" + str(seed) + "'"
        friendlist_in_database = pd.read_sql(sql=s, con=dbh.engine)["target"].tolist()
        friendlist_in_database = list(map(int, friendlist_in_database))
        self.assertEqual(friendlist_in_database, friendlist)

        if dbh.config.dbtype == "sqlite":
            dbh.engine.close()

    def test_sql_connection_raises_error_if_credentials_are_wrong(self):
        wrong_cfg = copy.deepcopy(self.config_dict_mysql)
        wrong_cfg["sql"]["passwd"] = "wrong"
        with open('config.yml', 'w') as f:
            yaml.dump(wrong_cfg, f, default_flow_style=False)

        with self.assertRaises(OperationalError):
            DataBaseHandler()

    def test_mysql_db_connects_and_creates_friends_table_with_correct_columns(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.config_dict_mysql, f, default_flow_style=False)

        dbh = DataBaseHandler()
        engine = dbh.engine
        s = "SELECT * FROM friends"
        sql_out = pd.read_sql(sql=s, con=engine).columns
        self.assertIn("source", sql_out)
        self.assertIn("target", sql_out)
        self.assertIn("burned", sql_out)
        self.assertIn("timestamp", sql_out)

        s = "SELECT * FROM result"
        sql_out = pd.read_sql(sql=s, con=engine).columns
        self.assertIn("source", sql_out)
        self.assertIn("target", sql_out)
        self.assertIn("timestamp", sql_out)

        engine.execute("DROP TABLES friends, user_details, result;")

    @skipIfDraining()
    def test_dbh_write_friends_function_also_works_with_mysql(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.config_dict_mysql, f, default_flow_style=False)
        seed = int(FileImport().read_seed_file().iloc[0])
        dbh = DataBaseHandler()
        c = Collector(Connection(), seed)
        friendlist = c.get_friend_list()
        friendlist = list(map(int, friendlist))

        dbh.write_friends(seed, friendlist)
        engine = dbh.engine
        s = "SELECT target FROM friends WHERE source LIKE '" + str(seed) + "'"
        friendlist_in_database = pd.read_sql(sql=s, con=engine)["target"].tolist()
        friendlist_in_database = list(map(int, friendlist_in_database))
        self.assertEqual(sorted(friendlist_in_database), sorted(friendlist))

        # This is to clean up.
        engine.execute("DROP TABLE friends;")

    def test_dbh_user_details_table_is_not_created_if_user_details_config_empty(self):
        # First for mysql
        no_user_details_cfg = copy.deepcopy(self.config_dict_mysql)
        no_user_details_cfg["twitter_user_details"] = dict(
            id=None,
            followers_count=None,
            status_lang=None,
            time_zone=None
        )
        with open("config.yml", "w") as f:
            yaml.dump(no_user_details_cfg, f, default_flow_style=False)
        dbh = DataBaseHandler()
        engine = dbh.engine
        with self.assertRaises(ProgrammingError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=engine)
        engine.execute("DROP TABLE friends;")

        # Second for sqlite
        cfg = copy.deepcopy(self.config_dict_sqlite)
        cfg["twitter_user_details"] = dict(
            id=None,
            followers_count=None,
            status_lang=None,
            time_zone=None
        )
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)
        config = Config()
        conn = lite.connect(config.dbname + ".db")
        with self.assertRaises(DatabaseError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=conn)
        conn.close()

    def test_twitter_user_details_not_in_config_as_key(self):
        # First for mysql
        no_key_cfg = copy.deepcopy(self.config_dict_mysql)
        no_key_cfg.pop('twitter_user_details', None)
        with open("config.yml", "w") as f:
            yaml.dump(no_key_cfg, f, default_flow_style=False)
        dbh = DataBaseHandler()
        engine = dbh.engine
        with self.assertRaises(ProgrammingError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=engine)
        engine.connect().execute("DROP TABLE friends;")

        # Second for sqlite
        cfg = copy.deepcopy(self.config_dict_sqlite)
        cfg.pop('twitter_user_details', None)
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)
        config = Config()
        conn = lite.connect(config.dbname + ".db")
        DataBaseHandler()

        with self.assertRaises(DatabaseError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=conn)
        conn.close()

    def test_dbh_user_details_is_not_created_if_user_details_config_empty(self):
        # First for mysql
        cfg = self.config_dict_mysql.copy()
        cfg["twitter_user_details"] = dict(
            id=None,
            followers_count=None,
            status_lang=None,
            time_zone=None
        )
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)
        config = Config()
        engine = create_engine(
            f'mysql+pymysql://{config.dbuser}:{config.dbpwd}@{config.dbhost}/{config.dbname}')
        DataBaseHandler()

        with self.assertRaises(ProgrammingError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=engine)
        engine.connect().execute("DROP TABLE friends;")

        # Second for sqlite
        cfg = self.config_dict_sqlite.copy()
        cfg["twitter_user_details"] = dict(
            id=None,
            followers_count=None,
            status_lang=None,
            time_zone=None
        )
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)
        config = Config()
        conn = lite.connect(config.dbname + ".db")
        with self.assertRaises(DatabaseError):
            s = "SELECT * FROM user_details"
            pd.read_sql(sql=s, con=conn)
        conn.close()

    def test_user_details_table_is_created_and_contains_columns_indicated_in_config(self):
        # First for mysql
        cfg = test_helpers.config_dict_user_details_dtypes_mysql
        with open('config.yml', 'w') as f:
            yaml.dump(cfg, f, default_flow_style=False)
        dbh = DataBaseHandler()
        engine = dbh.engine
        s = "SELECT * FROM user_details"
        cols = pd.read_sql(sql=s, con=engine).columns
        self.assertIn("id", cols)
        self.assertIn("followers_count", cols)
        self.assertIn("status_lang", cols)
        self.assertIn("timestamp", cols)
        engine.connect().execute("DROP TABLES friends, user_details;")

        # Second for sqlite
        cfg = test_helpers.config_dict_user_details_dtypes_sqlite
        with open('config.yml', 'w') as f:
            yaml.dump(cfg, f, default_flow_style=False)
        dbh = DataBaseHandler()
        conn = lite.connect(dbh.config.dbname + ".db")
        s = "SELECT * FROM user_details"
        cols = pd.read_sql(sql=s, con=conn).columns
        self.assertIn("id", cols)
        self.assertIn("followers_count", cols)
        self.assertIn("status_lang", cols)
        self.assertIn("timestamp", cols)
        conn.close()

    def test_all_user_details_can_be_written_using_given_dtypes(self):
        cfg = test_helpers.config_dict_user_details_dtypes_mysql
        select_vars = list(cfg["twitter_user_details"].keys())
        json_list = []
        for filename in os.listdir(os.path.join("tests", "tweet_jsons")):
            with open(os.path.join("tests", "tweet_jsons", filename), "r") as f:
                json_list.append(json.load(f))

        friends_details = Collector.make_friend_df(json_list,
                                                   select=select_vars,
                                                   provide_jsons=True)

        dbh = DataBaseHandler(config_dict=cfg)
        datetypes = helpers.friends_details_dtypes
        select_items = cfg["twitter_user_details"].items()

        try:
            friends_details.to_sql('user_details', if_exists='append',
                                   index=False, con=dbh.engine)
        # Error Handling for duplicate IDs
        except IntegrityError:
            friends_details.drop_duplicates(subset="id", keep='last', inplace=True)
            friends_details.to_sql('user_details', if_exists='append',
                                   index=False, con=dbh.engine)
        s = "SELECT * FROM user_details"
        sql_friends_details = pd.read_sql(sql=s, con=dbh.engine)

        for key, value in datetypes.items():
            if value == bool:
                friends_details[key] = friends_details[key].fillna(False).astype(value)

        # Dropping timestamp column and make boolean vars comparable
        # Remember that 'Null'.astype('bool') = True (thats why NAs first have to be filled)
        sql_friends_details.drop(columns="timestamp", inplace=True)
        for var, val in select_items:
            # If orginally boolean, make both dtypes int8 so the test succeeds
            if val == "SMALLINT":
                friends_details[var] = friends_details[var].fillna(-1)
                sql_friends_details[var] = sql_friends_details[var].astype('int8')
                friends_details[var] = friends_details[var].astype('int8')

        # Sorting dfs + indices so they have the same structure
        sql_friends_details.sort_index(axis=1, inplace=True)
        sql_friends_details.sort_values("id", inplace=True)
        friends_details.sort_index(axis=1, inplace=True)
        friends_details.sort_values("id", inplace=True)
        friends_details.reset_index(drop=True, inplace=True)
        assert_frame_equal(friends_details, sql_friends_details)

    def test_make_temp_tbl_makes_a_temp_tbl_equal_to_type(self):
        cfg = test_helpers.config_dict_user_details_dtypes_mysql
        dbh = DataBaseHandler(config_dict=cfg)
        temp_tbl_name = dbh.make_temp_tbl()

        user_details_colnames = pd.read_sql("SELECT * FROM user_details;",
                                            con=dbh.engine).columns
        temp_tbl_colnames = pd.read_sql("SELECT * FROM " + temp_tbl_name + ";",
                                        con=dbh.engine).columns

        assert_index_equal(user_details_colnames, temp_tbl_colnames)
        self.assertIsInstance(temp_tbl_name, str)

        dbh.engine.execute("DROP TABLE " + temp_tbl_name + ";")


class ConfigTest(unittest.TestCase):

    def setUp(self):
        if os.path.isfile("config.yml"):
            os.rename("config.yml", "config.yml.bak")

    def tearDown(self):
        if os.path.isfile("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")

    config_dict = test_helpers.config_dict_sqlite

    def test_config_file_not_existing(self):
        # File not found
        with self.assertRaises(FileNotFoundError):
            Config()

    def test_config_parameters_default_values_get_set_if_not_given(self):
        cfg_dict = copy.deepcopy(self.config_dict)
        cfg_dict["sql"] = {key: None for (key, value) in self.config_dict["sql"].items()}
        config = Config(config_dict=cfg_dict)

        # If the db_name is not given, does the Config() class assign a default?
        self.assertEqual("new_database", config.dbname)
        # If the dbtype is not specified, does the Config() class assume sqlite?
        self.assertEqual("sqlite", config.dbtype)

    def test_correct_values_for_config_parameters_given(self):
        # dbtype does not match "sqlite" or "SQL"
        cfg_dict = copy.deepcopy(self.config_dict)
        cfg_dict["sql"] = dict(
            dbtype='notadbtype',
            host=None,
            user=2,
            passwd=None,
            dbname="test_db"
        )

        with self.assertRaises(ValueError):
            Config(config_dict=cfg_dict)

        # Error when dbhost not provided?
        cfg_dict["sql"] = dict(
            dbtype='mysql',
            host='',
            user='123',
            passwd='456',
            dbname="test_db"
        )

        with self.assertRaises(ValueError):
            Config(config_dict=cfg_dict)

        # Error when dbuser not provided?
        cfg_dict["sql"] = dict(
            dbtype='mysql',
            host='host@host',
            user='',
            passwd='456',
            dbname="test_db"
        )

        with self.assertRaises(ValueError):
            Config(config_dict=cfg_dict)

        # Error when dpwd not provided?
        cfg_dict["sql"] = dict(
            dbtype='mysql',
            host='host@host',
            user='123',
            passwd='',
            dbname="test_db"
        )

        with self.assertRaises(ValueError):
            Config(config_dict=cfg_dict)

    def test_config_file_gets_read_incl_all_keys(self):
        cfg_dict = copy.deepcopy(self.config_dict)
        cfg_dict["sql"] = {key: None for (key, value) in self.config_dict["sql"].items()}
        cfg_dict["twitter_user_details"] = {key: None
                                            for (key, value)
                                            in self.config_dict["twitter_user_details"].items()}
        cfg_dict["notifications"] = {key: None
                                     for (key, value)
                                     in self.config_dict["notifications"].items()}
        self.assertEqual(Config("tests/config_test_empty.yml").config, cfg_dict)

    def test_sql_key_not_in_config(self):
        cfg_dict = copy.deepcopy(self.config_dict)
        cfg_dict.pop("sql", None)
        config = Config(config_dict=cfg_dict)

        self.assertEqual("sqlite", config.config["sql"]["dbtype"])
        self.assertEqual("new_database", config.config["sql"]["dbname"])

    def test_notif_config_is_created_depending_on_config(self):
        cfg = copy.deepcopy(self.config_dict)
        cfg["notifications"] = {
            'email_to_notify': passwords.email_to_notify,
            'mailgun_default_smtp_login': passwords.mailgun_default_smtp_login,
            'mailgun_api_base_url': passwords.mailgun_api_base_url,
            'mailgun_api_key': passwords.mailgun_api_key
        }
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f)
        config = Config()

        self.assertEqual(cfg["notifications"], config.notif_config)
        self.assertTrue(config.use_notifications)

        cfg = copy.deepcopy(self.config_dict)
        cfg["notifications"] = {
            'email_to_notify': None,
            'mailgun_default_smtp_login': None,
            'mailgun_api_base_url': None,
            'mailgun_api_key': None
        }
        config = Config(config_dict=cfg)

        self.assertFalse(config.use_notifications)

        cfg = copy.deepcopy(self.config_dict)
        cfg["notifications"] = {
            'email_to_notify': passwords.email_to_notify,
            'mailgun_default_smtp_login': None,
            'mailgun_api_base_url': None,
            'mailgun_api_key': None
        }
        with open("config.yml", "w") as f:
            yaml.dump(cfg, f)

        with self.assertRaises(ValueError):
            config = Config()

    def test_send_test_error(self):
        cfg = test_helpers.config_dict_sqlite
        cfg["notifications"] = {
            'email_to_notify': passwords.email_to_notify,
            'mailgun_default_smtp_login': passwords.mailgun_default_smtp_login,
            'mailgun_api_base_url': passwords.mailgun_api_base_url,
            'mailgun_api_key': passwords.mailgun_api_key
        }
        config = Config(config_dict=cfg)
        response = config.send_mail({
            "subject": "Unexpected Error",
            "text": "Unexpected Error, please check whatever might be wrong.\n FULL ERROR HERE"
        }
        )

        self.assertIn('200', str(response))


class CollectorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.connection = Connection()

    def test_collector_raises_exception_if_credentials_are_wrong(self):
        with self.assertRaises(tweepy.TweepError) as te:
            connection = Connection(token_file_name="wrong_tokens.csv")
            connection.api.verify_credentials()

        exception = te.exception

        self.assertIn('401', str(exception.response))

    def test_token_queue_has_reset_time(self):
        connection = Connection()
        token = connection.token_queue.get()
        self.assertIsInstance(token[2], dict)

    def test_collector_can_connect_with_correct_credentials(self):

        try:
            self.connection.api.verify_credentials()
        except tweepy.TweepError:
            self.fail("Could not verify API credentials.")

    def test_collector_gets_remaining_API_calls_for_friendlist(self):
        remaining_calls = self.connection.remaining_calls()
        reset_time = self.connection.reset_time()

        self.assertGreaterEqual(remaining_calls, 0)
        self.assertLessEqual(remaining_calls, 15)

        self.assertGreaterEqual(reset_time, 0)
        self.assertLessEqual(reset_time, 900)

        if reset_time == 900:
            self.assertEqual(remaining_calls, 15)

    def test_collector_gets_remaining_API_calls_for_user_lookup(self):
        remaining_calls = self.connection.remaining_calls(endpoint='/users/lookup')
        reset_time = self.connection.reset_time(endpoint='/users/lookup')

        self.assertGreaterEqual(remaining_calls, 0)
        self.assertLessEqual(remaining_calls, 900)

        self.assertGreaterEqual(reset_time, 0)
        self.assertLessEqual(reset_time, 900)

        if reset_time == 900:
            self.assertEqual(remaining_calls, 900)

    def test_collector_gets_all_friends_of_power_user_gets_details_and_makes_df(self):

        collector = Collector(self.connection, seed=2343198944)

        user_friends = collector.get_friend_list()

        self.assertGreater(len(user_friends), 5000)

    @skipIfDraining()
    def test_collector_gets_all_friends_if_more_than_15_requests_needed(self):

        collector = Collector(self.connection, seed=14230524)  # Lady Gaga

        user_friends = collector.get_friend_list()

        self.assertIsInstance(user_friends, list)
        self.assertGreater(len(user_friends), 100000)
        self.assertLess(len(user_friends), 200000)

    def test_collector_gets_friend_details_and_makes_df(self):

        collector = Collector(self.connection, seed=36476777)

        user_friends = collector.get_friend_list()
        friends_details = collector.get_details(user_friends)

        self.assertGreaterEqual(len(friends_details), 100)

        friends_df = Collector.make_friend_df(friends_details)

        self.assertIsInstance(friends_df, pd.DataFrame)
        self.assertEqual(len(friends_df), len(friends_details))

        self.assertIsInstance(friends_df['id'][0], np.int64)
        self.assertIsInstance(friends_df['followers_count'][0], np.int64)
        self.assertIsInstance(friends_df['status_lang'][0], str)

        friends_df_selected = Collector.make_friend_df(friends_details,
                                                       select=['id', 'followers_count',
                                                               'created_at'])

        self.assertEqual(len(friends_df_selected.columns), 3)
        self.assertIsInstance(friends_df['id'][0], np.int64)

        # Date columns equal pd Timestamps b/c pandas stores all dates as datetime64[ns] format.
        self.assertIsInstance(friends_df['created_at'][0], pd.Timestamp)
        self.assertIsInstance(friends_df['followers_count'][0], np.int64)

    def test_next_token_works(self):

        collector = Collector(self.connection, seed=36476777)

        for i in range(len(collector.connection.tokens) + 1):
            # test whether it does not exceed the list

            old_token = collector.connection.token
            old_secret = collector.connection.secret

            collector.connection.next_token()

            self.assertNotEqual(old_token, collector.connection.token)
            self.assertNotEqual(old_secret, collector.connection.secret)

            try:
                self.connection.api.verify_credentials()
            except tweepy.TweepError:
                self.fail("Could not verify API credentials after token change.")

    @skipIfDraining()
    def test_get_friend_list_changes_token(self):

        for i in range(16):
            collector = Collector(self.connection, seed=36476777)
            try:
                collector.get_friend_list()
            except tweepy.TweepError:
                self.fail("Apparently the token change did not work.")

    def test_check_follow_back(self):

        collector = Collector(self.connection, seed=36476777)

        # FlxVctr follows BenAThies
        self.assertEqual(collector.check_follows(36476777, 83662933), True)

        # BarackObama does not follow FlxVctr
        self.assertEqual(collector.check_follows(813286, 36476777), False)

    def test_evade_key_errors_in_make_friend_df(self):
        json_list = []
        for filename in os.listdir(os.path.join("tests", "tweet_jsons")):
            with open(os.path.join("tests", "tweet_jsons", filename), "r") as f:
                json_list.append(json.load(f))

        df = Collector.make_friend_df(friends_details=json_list,
                                      provide_jsons=True)
        self.assertEqual(["id", "followers_count", "status_lang", "created_at",
                          "statuses_count"].sort(), list(df).sort())

    def test_retry_if_rate_limited(self):

        class TestCollector(Collector):

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                self.first_run = True

            @Collector.Decorators.retry_with_next_token_on_rate_limit_error
            def raise_rate_limit_once(self, arg, kwarg=0, force_retry_token=True):
                if self.first_run:
                    self.first_run = False
                    raise tweepy.RateLimitError("testing (this should not lead to a fail)")
                else:
                    return (arg, kwarg)

        collector = TestCollector(self.connection, seed=36476777)
        token = self.connection.token

        self.assertEqual(collector.raise_rate_limit_once(1, kwarg=2, force_retry_token=True),
                         (1, 2))
        self.assertNotEqual(token, self.connection.token)

    def test_invalid_token(self):

        connection = Connection()

        tokens = []

        while True:
            try:
                tokens.append(connection.token_queue.get(block=False))
            except queue.Empty:
                break

        connection.token_queue.put(('invalid', 'invalid', {}, {}))
        for token in tokens:
            connection.token_queue.put(token)
        connection.next_token()

        collector = Collector(connection, seed=36476777)

        try:
            collector.check_API_calls_and_update_if_necessary(endpoint='/friends/ids')
        except tweepy.error.TweepError as e:
            self.fail(e)

        self.assertIsInstance(collector.check_API_calls_and_update_if_necessary(
            endpoint='/friends/ids'), int)

    def test_check_API_updates_reset_time(self):
        connection = Connection()
        collector = Collector(connection, seed=36476777)

        tokens = []

        while True:
            try:
                tokens.append(connection.token_queue.get(block=False))
            except queue.Empty:
                break

        connection.token_queue.put(tokens[-1])

        connection.calls_dict['/friends/ids'] = 1

        collector.check_API_calls_and_update_if_necessary(endpoint='/friends/ids',
                                                          check_calls=False)

        tokentuple = connection.token_queue.get()

        self.assertEqual(tokentuple[3]['/friends/ids'], 0)
        self.assertGreater(tokentuple[2]['/friends/ids'], time.time())


''' TODO: make this work (add nonetype_replace param to flatten_json)
    def test_flatten_json_nonetype_param_works_as_expected(self):
        testdict = {
            "testy":[1, 2, 3, 4, 5],
            "mc": None,
            "testtest": {
                "johnny": ["what", "a", "nice", "list", "of", "strings"],
                "mary": None,
                "robert": 1337
            }
        }
        cols = ["testy", "mc", "mary", "robert", "testtest_johnny"]
        expected_dict = {
            'testy': '[1, 2, 3, 4, 5]',
            'mc': -1,
            'testtest_johnny': "['what', 'a', 'nice', 'list', 'of', 'strings']",
            'testtest_mary': -1,
            'testtest_robert': 1337
        }
        flat_dict = flatten_json(testdict, columns=cols, nonetype=-1)
        print(flat_dict)
        self.assertEqual(flat_dict, expected_dict)
'''


class CoordinatorTest(unittest.TestCase):

    config_dict_sql = test_helpers.config_dict_mysql
    mock_sql_cfg = copy.deepcopy(config_dict_sql)
    config_dict_sqlite = test_helpers.config_dict_sqlite
    mock_sqlite_cfg = copy.deepcopy(config_dict_sqlite)

    db_name = Config(config_dict=config_dict_sql).dbname

    @classmethod
    def setUpClass(self):
        if os.path.isfile("seeds.csv"):
            os.rename("seeds.csv", "seeds.csv.bak")
        os.rename("seeds_test.csv", "seeds.csv")

        if os.path.isfile("config.yml"):
            os.rename("config.yml", "config.yml.bak")

        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sql_cfg, f, default_flow_style=False)

        self.dbh = DataBaseHandler(config_dict=test_helpers.config_dict_mysql)
        self.seed_list = [2367431, 36476777]

    @classmethod
    def tearDownClass(self):
        os.rename("seeds.csv", "seeds_test.csv")
        if os.path.isfile("seeds.csv.bak"):
            os.rename("seeds.csv.bak", "seeds.csv")

        if os.path.isfile("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")

        try:
            self.coordinator.seed_queue.close()
            self.coordinator.seed_queue.join_thread()
        except AttributeError:
            pass

    def setUp(self):
        self.coordinator = Coordinator(seed_list=self.seed_list)

    def tearDown(self):
        try:
            self.dbh.engine.execute("DROP TABLE friends;")
        except Exception:
            pass

        try:
            self.dbh.engine.execute("DROP TABLE user_details;")
        except Exception:
            pass

        try:
            self.dbh.engine.execute("DROP TABLE result;")
        except Exception:
            pass

    def test_coordinator_selects_n_random_seeds(self):
        coordinator = Coordinator(seeds=10)
        self.assertEqual(len(coordinator.seeds), 10)
        try:
            coordinator.seed_queue.close()
            coordinator.seed_queue.join_thread()
        except AttributeError:
            pass
        coordinator = Coordinator(seeds=2)
        self.assertEqual(len(coordinator.seeds), 2)
        try:
            coordinator.seed_queue.close()
            coordinator.seed_queue.join_thread()
        except AttributeError:
            pass

    def test_can_get_seed_from_queue(self):
        coordinator = Coordinator(seeds=2)

        self.assertIsInstance(coordinator.seed_queue.get(), np.int64)
        self.assertIsInstance(coordinator.seed_queue.get(), np.int64)
        self.assertTrue(coordinator.seed_queue.empty())

        try:
            coordinator.seed_queue.close()
            coordinator.seed_queue.join_thread()
        except AttributeError:
            pass

    def test_can_get_token_from_queue(self):
        coordinator = Coordinator()

        tokentuple = coordinator.token_queue.get()
        self.assertIsInstance(tokentuple, tuple)
        self.assertIsInstance(tokentuple[0], str)
        self.assertIsInstance(tokentuple[1], str)
        self.assertIsInstance(tokentuple[2], dict)
        tokentuple = coordinator.token_queue.get()
        self.assertIsInstance(tokentuple, tuple)
        self.assertIsInstance(tokentuple[0], str)
        self.assertIsInstance(tokentuple[1], str)
        self.assertIsInstance(tokentuple[2], dict)

    def test_db_can_lookup_friends(self):

        # write some friends in db
        seed = self.coordinator.seed_queue.get()

        connection = Connection()
        c = Collector(connection, seed)
        friendlist = c.get_friend_list()

        self.dbh.write_friends(seed, friendlist)

        friends_details = c.get_details(friendlist)

        select_list = ['created_at', 'followers_count', 'id', 'status_lang', 'statuses_count']
        friends_details = Collector.make_friend_df(friends_details, select=select_list)
        friends_details.to_sql('user_details', if_exists='append',
                               index=False, con=self.coordinator.dbh.engine)

        friends_details_lookup = self.coordinator.lookup_accounts_friend_details(
            seed, self.coordinator.dbh.engine, select=", ".join(select_list) + ", timestamp")

        # self.coordinator.seed_queue.close()
        # self.coordinator.seed_queue.join_thread()

        friends_details.reindex(sorted(friends_details.columns), axis=1)
        friends_details.sort_values(by=['id'], inplace=True)
        friends_details.reset_index(drop=True, inplace=True)
        friends_details_lookup.reindex(sorted(friends_details_lookup.columns), axis=1)
        friends_details_lookup.sort_values('id', inplace=True)
        friends_details_lookup.reset_index(drop=True, inplace=True)
        friends_details_lookup.drop('timestamp', axis='columns', inplace=True)

        assert_frame_equal(friends_details, friends_details_lookup)

        friends_details_lookup_fail = self.coordinator.lookup_accounts_friend_details(
            0, self.dbh.engine)

        self.assertFalse(friends_details_lookup_fail)

        self.dbh.engine.execute("DROP TABLES friends, user_details;")

    def test_work_through_seed(self):

        seed = 36476777
        expected_new_seed = 813286
        expected_new_seed_2 = 783214  # after first got burned
        # there's no database, test getting seed via Twitter API
        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, retries=1)
        # Felix's most followed 'friend' is BarackObama
        self.assertEqual(new_seed, expected_new_seed)

        # destroy Twitter connection and rely on database
        try:
            new_seed = self.coordinator.work_through_seed_get_next_seed(seed,
                                                                        connection="fail",
                                                                        retries=1)
            self.assertEqual(new_seed, expected_new_seed_2)
        except AttributeError:
            self.fail("could not retrieve friend details from database")

        # test whether seed->new_seed connection is in database
        query = f"""
                SELECT source, target FROM result WHERE source = {seed}
                """
        edge = pd.read_sql(query, con=self.dbh.engine)
        self.assertIn(new_seed, edge['target'].values)

        # TODO: test follow-back

        # test whether connection is burned in friendlist

        query = f"""
                SELECT burned FROM friends WHERE source = {seed} AND target = {new_seed}
                """
        burned_edge = pd.read_sql(query, con=self.dbh.engine)
        self.assertEqual(len(burned_edge), 1)
        self.assertEqual(burned_edge['burned'].values[0], 1)

        # test whether burned connection will not be returned again
        burned_seed = new_seed
        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, retries=1)
        self.assertNotEqual(new_seed, burned_seed)

        # test whether burned connection will not be returned again after restart
        burned_seed = new_seed
        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, retries=2, restart=True)
        self.assertNotEqual(new_seed, expected_new_seed)

    def test_work_through_seed_if_account_has_no_friends(self):

        seed = 770602317242523648

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, retries=1)

        self.assertIsInstance(new_seed, np.int64)

    def test_work_through_seed_if_account_is_protected(self):

        seed = 557558765

        with self.assertRaises(tweepy.error.TweepError,
                               msg="User with id {} is not protected anymore.".format(seed)):
            connection = Connection()
            c = Collector(connection, seed)
            if c.connection.token[:8] == "36476777":
                c.connection.next_token()
            c.get_friend_list()

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, retries=1)

        self.assertIsInstance(new_seed, np.int64)

    def test_work_through_seed_if_seed_does_not_exist(self):

        seed = 1040403366

        with self.assertRaises(tweepy.error.TweepError,
                               msg="User with id {} exists.".format(seed)):
            connection = Connection()
            c = Collector(connection, seed)
            c.get_friend_list()

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed, test_fail=True,
                                                                    retries=1)

        self.assertIsInstance(new_seed, np.int64)

    def test_work_through_seed_twice_if_account_has_no_friends_speaking_language(self):

        seed = 1621528116

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed,
                                                                    status_lang='de',
                                                                    retries=1)

        self.assertIsInstance(new_seed, np.int64)

        friends_details = self.coordinator.lookup_accounts_friend_details(
            seed, self.dbh.engine)

        self.assertEqual(0, len(friends_details))

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed,
                                                                    status_lang='de',
                                                                    retries=1)

        self.assertIsInstance(new_seed, np.int64)

    def test_work_through_seed_who_should_have_german_friends(self):

        seed = 36476777

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed,
                                                                    status_lang='de',
                                                                    retries=1)

        self.assertIsInstance(new_seed, np.int64)

        friends_details = self.coordinator.lookup_accounts_friend_details(
            seed, self.dbh.engine)

        self.assertGreater(len(friends_details), 0)

    def test_work_through_seed_who_should_have_german_and_english_friends(self):

        seed = 36476777

        new_seed = self.coordinator.work_through_seed_get_next_seed(seed,
                                                                    status_lang=['de', 'en'],
                                                                    retries=1)

        self.assertIsInstance(new_seed, np.int64)

        friends_details = self.coordinator.lookup_accounts_friend_details(
            seed, self.dbh.engine)

        self.assertGreater(len(friends_details), 0)
        self.assertIn('de', friends_details['status_lang'].values)
        self.assertIn('en', friends_details['status_lang'].values)

    def test_start_collectors(self):

        seeds = set(self.seed_list)
        expected_new_seeds = {9334352, 813286}

        processes = self.coordinator.start_collectors(retries=1)

        self.assertEqual(len(processes), 2)

        saved_seeds = pd.read_csv('latest_seeds.csv', header=None)

        saved_seeds = set(saved_seeds[0].values)

        self.assertEqual(seeds, saved_seeds)

        for process in processes:
            self.assertIsInstance(process, mp.Process, msg="type is {}".format(type(process)))
            stdout.write("Waiting for processes to finish.")
            stdout.flush()
            process.join(timeout=1200)

        new_seeds = set()

        for i in range(2):
            new_seeds.add(self.coordinator.seed_queue.get(timeout=1000))

        self.assertEqual(len(new_seeds), 2)

        self.assertNotEqual(new_seeds, seeds)
        self.assertEqual(new_seeds, expected_new_seeds)

    def test_bootstrap(self):

        coordinator_with_bootstrap_enabled = Coordinator(seed_list=[36476777],
                                                         following_pages_limit=1)
        number_of_seeds_in_pool = len(coordinator_with_bootstrap_enabled.seed_pool)

        processes = coordinator_with_bootstrap_enabled.start_collectors(retries=1, bootstrap=True,
                                                                        status_lang='de')

        processes[0].join(timeout=1000)

        processes = coordinator_with_bootstrap_enabled.start_collectors(retries=1, bootstrap=True,
                                                                        status_lang='de')

        processes[0].join(timeout=1000)

        new_number_of_seeds_in_pool = len(coordinator_with_bootstrap_enabled.seed_pool)

        self.assertGreater(new_number_of_seeds_in_pool, number_of_seeds_in_pool)
        self.assertEqual(len(coordinator_with_bootstrap_enabled.seed_pool.columns), 1)

    def test_overlapping_friends(self):

        coordinator = Coordinator(seed_list=[36476777, 83662933, 2367431])
        worker_bees = coordinator.start_collectors(retries=1)

        for bee in worker_bees:
            bee.join(timeout=1200)
            if bee.err is not None:
                raise bee.err

    def test_2_girls_1_cup(self):

        director = Coordinator(seed_list=[36476777, 36476777])
        cup = director.start_collectors(retries=1)

        for girl in cup:
            girl.join(timeout=1200)
            if girl.err is not None:
                raise girl.err

        # TODO: find a way to test with an assertion whether this works correctly

    def test_failing_work_through_seed(self):

        c = Coordinator(seed_list=[36476777, 83662933, 2367431])

        workers = c.start_collectors(fail_hidden=True, retries=2)

        for worker in workers:
            worker.join()
            with self.assertRaises(TestException):
                if worker.err is not None:
                    raise worker.err

    def test_main_loop_resets_db_after_restart(self):

        coordinator = Coordinator(seed_list=[36476777],
                                  following_pages_limit=1)

        main_loop(coordinator, status_lang='de')

        timestamp_test = pd.read_sql(
            f"SELECT UNIX_TIMESTAMP(timestamp) < {time.time()} \
            FROM result WHERE source = 36476777",
            con=self.dbh.engine).values[0][0]

        self.assertEqual(timestamp_test, 1)

        timestamp_test = pd.read_sql(
            f"SELECT UNIX_TIMESTAMP(timestamp) > {time.time() - 60} \
            FROM result WHERE source = 36476777",
            con=self.dbh.engine).values[0][0]

        self.assertEqual(timestamp_test, 1)

        result = pd.read_sql(sql="SELECT * from result", con=self.dbh.engine)

        latest_seeds_df = pd.read_csv('latest_seeds.csv', header=None)[0]
        latest_seeds = list(latest_seeds_df.values)
        coordinator = Coordinator(seed_list=latest_seeds,
                                  following_pages_limit=1)

        main_loop(coordinator, status_lang='de', restart=True)

        result_after_restart = pd.read_sql(sql="SELECT * from result", con=self.dbh.engine)

        pd.testing.assert_frame_equal(result, result_after_restart)

    def test_main_loop_with_bootstrap(self):

        coordinator = Coordinator(seed_list=[36476777],
                                  following_pages_limit=1)

        seed_pool_size = len(coordinator.seed_pool)

        for i in range(2):
            main_loop(coordinator, status_lang='de', bootstrap=True)

        middle_seed_pool_size = len(coordinator.seed_pool)
        self.assertGreater(middle_seed_pool_size, seed_pool_size)

        latest_seeds_df = pd.read_csv('latest_seeds.csv', header=None)[0]
        latest_seeds = list(latest_seeds_df.values)
        new_coordinator = Coordinator(seed_list=latest_seeds, following_pages_limit=1)

        main_loop(new_coordinator, status_lang='de', bootstrap=True, restart=True)

        last_seed_pool_size = len(new_coordinator.seed_pool)
        self.assertGreater(last_seed_pool_size, middle_seed_pool_size)


class GeneralTests(unittest.TestCase):

    def test_can_get_account_tweets(self):

        connection = Connection()
        user_id = 36476777

        latest_tweets = get_latest_tweets(user_id, connection)

        self.assertIsInstance(latest_tweets, pd.DataFrame)
        self.assertGreater(len(latest_tweets), 0)
        self.assertIsInstance(latest_tweets['status_lang'], pd.Series)
        self.assertIsInstance(latest_tweets['text'], pd.Series)

    def test_retry_decorator(self):

        self.first_run = 1

        @retry_x_times(4)
        def fail_once():
            if self.first_run <= 3:
                self.first_run += 1
                raise TestException
            return 0

        self.assertEqual(0, fail_once())


if __name__ == "__main__":
    unittest.main()
