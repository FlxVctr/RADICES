import json
import os
import shutil
import tweepy
import unittest
import yaml
import pandas as pd
from collector import Connection, Collector
from database_handler import DataBaseHandler
from json import JSONDecodeError
from pandas.api.types import is_string_dtype
from pandas.errors import EmptyDataError
from setup import Config, FileImport
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from subprocess import Popen, PIPE, check_output
from twauth import OAuthorizer


def setUpModule():
    if os.path.isfile("config.yml"):
        os.rename("config.yml", "config_bak.yml")


def tearDownModule():
    if os.path.isfile("config_bak.yml"):
        os.replace("config_bak.yml", "config.yml")


class FileImportTest(unittest.TestCase):

    def setUp(self):
        if os.path.isfile("keys.json"):
            os.rename("keys.json", "keys_bak.json")
        if os.path.isfile("seeds.csv"):
            os.rename("seeds.csv", "seeds_bak.csv")

    def tearDown(self):
        if os.path.isfile("keys_bak.json"):
            os.replace("keys_bak.json", "keys.json")
        if os.path.isfile("seeds_bak.csv"):
            os.replace("seeds_bak.csv", "seeds.csv")

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
        if os.path.isfile("keys_bak.json"):
            os.replace("keys_bak.json", "keys.json")
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
    db_name = Config().dbname
    mock_sql_cfg = dict(
                sql=dict(
                    dbtype='mysql',
                    host='127.0.0.1',
                    user='root',
                    passwd='1q2w3e4r5t6z',
                    dbname="sparsetwitter"
                )
            )
    mock_sqlite_cfg = dict(
        sql=dict(
            dbtype='sqlite',
            host='',
            user='',
            passwd='',
            dbname="test_db"
        )
    )

    def tearDown(self):
        if os.path.isfile(self.db_name + ".db"):
            os.remove(self.db_name + ".db")

    def test_setup_and_database_handler_creates_database_from_given_name(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sqlite_cfg, f, default_flow_style=False)

        DataBaseHandler()
        db_name = self.db_name
        try:
            self.assertTrue(os.path.isfile(db_name + ".db"))
        except AssertionError:
            print("Database was not created!")

    def test_dbh_creates_friends_table_with_correct_columns(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sqlite_cfg, f, default_flow_style=False)
        DataBaseHandler()
        db_name = self.db_name
        cmd = "sqlite3 " + db_name + ".db .tables"
        response = str(check_output(cmd, shell=True))
        self.assertIn("friends", response)
        response = str(check_output("sqlite3 " + db_name + ".db < check_db_cmd.txt", shell=True))
        self.assertIn("id", response)
        self.assertIn("friend", response)
        self.assertIn("user", response)
        self.assertIn("burned", response)

    def test_dbh_write_friends_function_takes_input_and_writes_to_table(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sqlite_cfg, f, default_flow_style=False)
        seed = int(FileImport().read_seed_file().iloc[0])
        dbh = DataBaseHandler()
        c = Collector(Connection(), seed)
        friendlist = c.get_friend_list()
        friendlist = list(map(int, friendlist))

        dbh.write_friends(seed, friendlist)

        s = "SELECT friend FROM friends WHERE user LIKE '" + str(seed) + "'"
        friendlist_in_database = pd.read_sql(sql=s, con=dbh.conn)["friend"].tolist()
        friendlist_in_database = list(map(int, friendlist_in_database))
        self.assertEqual(friendlist_in_database, friendlist)

    def test_sql_connection_raises_error_if_credentials_are_wrong(self):
        new_mock_cfg = self.mock_sql_cfg
        new_mock_cfg["sql"]["passwd"] = "wrong"
        with open('config.yml', 'w') as f:
            yaml.dump(new_mock_cfg, f, default_flow_style=False)

        with self.assertRaises(OperationalError):
            DataBaseHandler()

    def test_mysql_db_connects_and_creates_friends_database_with_correct_columns(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sql_cfg, f, default_flow_style=False)

        config = Config()
        DataBaseHandler()
        engine = create_engine(
            'mysql+pymysql://' + config.dbuser + ':' + config.dbpwd + '@' +
            config.dbhost + '/' + config.dbname)

        s = "SELECT * FROM friends"
        sql_out = pd.read_sql(sql=s, con=engine)
        cols = list(sql_out)

        self.assertIn("id", cols)
        self.assertIn("user", cols)
        self.assertIn("friend", cols)
        self.assertIn("burned", cols)

        engine.connect().execute("DROP TABLE friends;")

    def test_dbh_write_friends_function_also_works_with_mysql(self):
        with open('config.yml', 'w') as f:
            yaml.dump(self.mock_sql_cfg, f, default_flow_style=False)
        seed = int(FileImport().read_seed_file().iloc[0])
        dbh = DataBaseHandler()
        config = Config()
        c = Collector(Connection(), seed)
        friendlist = c.get_friend_list()
        friendlist = list(map(int, friendlist))

        dbh.write_friends(seed, friendlist)

        engine = create_engine(
            'mysql+pymysql://' + config.dbuser + ':' + config.dbpwd + '@' +
            config.dbhost + '/' + config.dbname)
        s = "SELECT friend FROM friends WHERE user LIKE '" + str(seed) + "'"
        friendlist_in_database = pd.read_sql(sql=s, con=engine)["friend"].tolist()
        friendlist_in_database = list(map(int, friendlist_in_database))
        self.assertEqual(friendlist_in_database, friendlist)

        # This is to clean up.
        engine.connect().execute("DROP TABLE friends;")


# class OAuthTest(unittest.TestCase):
    # TODO: really necessary?
    # def test_oauth_throws_error_when_wrong_or_no_verifier(self):
    #     print("PLEASE ENTER RANDOM NUMBER")
    #     with self.assertRaises(tweepy.TweepError):
    #         OAuthorizer()

    # def test_new_line_in_csv_after_verifying(self):
    #     pass


class ConfigTest(unittest.TestCase):
    def test_1_config_file_gets_read_and_is_complete(self):
        # File not found
        with self.assertRaises(FileNotFoundError):
            Config()

    def test_2_make_config_works_as_expected(self):
        # Does make_config.py not make a new config.yml when entered "n"?
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=True)
        p.communicate("n\n".encode())
        self.assertFalse(os.path.isfile("config.yml"))

        # Does make_config.py open a dialogue asking to open the new config.yaml?
        # (Just close the dialogue)
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=True)
        p.communicate("y\n".encode())

    def test_3_config_parameters_default_values_get_set_if_not_given(self):
        # If the db_name is not given, does the Config() class assign a default?
        self.assertEqual("new_relations_database", Config().dbname)

        # If the dbtype is not specified, does the Config() class assume sqlite?
        self.assertEqual("sqlite", Config().dbtype)

    def test_4_correct_values_for_config_parameters_given(self):
        # dbtype does not match "sqlite" or "SQL"
        mock_cfg = dict(
            sql=dict(
                dbtype='notadbtype',
                host='',
                user=2,
                passwd='',
                dbname="test_db"
            )
        )
        with open('config.yml', 'w') as f:
            yaml.dump(mock_cfg, f, default_flow_style=False)
        with self.assertRaises(ValueError):
            Config()

        # Error when dbhost not provided?
        mock_cfg = dict(
            sql=dict(
                dbtype='mysql',
                host='',
                user='123',
                passwd='456',
                dbname="test_db"
            )
        )
        with open('config.yml', 'w') as f:
            yaml.dump(mock_cfg, f, default_flow_style=False)
        with self.assertRaises(ValueError):
            Config()

        # Error when dbuser not provided?
        mock_cfg = dict(
            sql=dict(
                dbtype='mysql',
                host='host@host',
                user='',
                passwd='456',
                dbname="test_db"
            )
        )
        with open('config.yml', 'w') as f:
            yaml.dump(mock_cfg, f, default_flow_style=False)
        with self.assertRaises(ValueError):
            Config()

        # Error when dpwd not provided?
        mock_cfg = dict(
            sql=dict(
                dbtype='mysql',
                host='host@host',
                user='123',
                passwd='',
                dbname="test_db"
            )
        )
        with open('config.yml', 'w') as f:
            yaml.dump(mock_cfg, f, default_flow_style=False)
        with self.assertRaises(ValueError):
            Config()

    def test_5_config_file_gets_read_correctly(self):
        if os.path.isfile("config_bak.yml"):
            shutil.copyfile("config_bak.yml", "config.yml")

    # Does the sql configuration match the current working standard?
    def test_config_file_gets_read_incl_all_fields(self):
        config_dict = {
         'sql': {
            'dbtype': "sqlite",
            'host': None,
            'user': None,
            'passwd': None,
            'dbname': 'test_db'
            }
        }
        self.assertEqual(Config().config, config_dict)


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

        friends_details = collector.get_details(user_friends)

        self.assertGreaterEqual(len(friends_details), 100)

        friends_df = Collector.make_friend_df(friends_details)

        self.assertIsInstance(friends_df, pd.DataFrame)
        self.assertEqual(len(friends_df), len(friends_details))

    def test_next_token_works(self):

        collector = Collector(self.connection, seed=36476777)

        old_token = collector.connection.token
        old_secret = collector.connection.secret

        collector.connection.next_token()

        self.assertNotEqual(old_token, collector.connection.token)
        self.assertNotEqual(old_secret, collector.connection.secret)

        try:
            self.connection.api.verify_credentials()
        except tweepy.TweepError:
            self.fail("Could not verify API credentials after token change.")


if __name__ == "__main__":
    unittest.main()
