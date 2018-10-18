import tweepy
import pandas as pd
import unittest
import os
import json
import yaml
import shutil
from setup import FileImport, Config
from json import JSONDecodeError
from database_handler import DataBaseHandler
from pandas.errors import EmptyDataError
from collector import Connection
from collector import Collector
import tweepy
import pandas as pd
from pandas.api.types import is_string_dtype
from subprocess import Popen, PIPE


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


class DatabaseHandlerTest(unittest.TestCase):

    DBH = DataBaseHandler()
    cfg = Config()


    def test_DBH_creates_new_database_if_none_exists(self):
        input("wait")
        self.assertTrue(os.path.exists(self.cfg.dbname + ".db"))

    def test_DBH_connects_to_old_database_if_name_exists(self):
        pass

    def test_asks_whether_to_overwrite_existing_database_with_new_one(self):
        pass

    def tearDown(self):
        if os.path.isfile(self.cfg.dbname + ".db"):
            os.remove(self.cfg.dbname + ".db")

    # TODO: test what happens if lite.connect in databasehandler fails.

#    def test_database_handler_creates_database_from_given_name(self, db_name=db_name):
#        dbh = DataBaseHandler()
#        dbh.new_db(db_name=db_name)
#        try:
#            self.assertTrue(os.path.isfile(self.db_name + ".db"))
#        except AssertionError:
#            print("Database was not created!")


class ConfigTest(unittest.TestCase):

    def test_1_config_file_gets_read_and_is_complete(self):
        # File not found
        with self.assertRaises(FileNotFoundError):
            Config()

    def test_2_make_config_works_as_expected(self):
        # Does make_config.py not make a new config.yml when entered "n"?
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE)
        p.communicate("n\n".encode())
        self.assertFalse(os.path.isfile("config.yml"))

        # Does make_config.py open a dialogue asking to open the new config.yaml?
        # (Just close the dialogue)
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE)
        p.communicate("y\n".encode())

    def test_3_config_parameters_default_values_get_set_if_not_given(self):
        # If the db_name is not given, does the Config() class assign a default?
        self.assertEqual("new_relations_database", Config().dbname)

        # If the dbtype is not specified, does the Config() class assume sqlite?
        self.assertEqual("sqlite", Config().dbtype)

    def test_4_correct_values_for_config_parameters_given(self):
        # dbtype does not match "sqlite" or "SQL"
        mock_cfg = dict(
            mysql=dict(
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
            mysql=dict(
                dbtype='SQL',
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
            mysql=dict(
                dbtype='SQL',
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
            mysql=dict(
                dbtype='SQL',
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

        # Does the mysql configuration match the current working standard?
        config_dict = {
         'mysql': {
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

    def test_collector_gets_remaining_API_calls(self):
        remaining_calls = self.connection.remaining_calls()
        reset_time = self.connection.reset_time()

        self.assertGreaterEqual(remaining_calls, 0)
        self.assertLessEqual(remaining_calls, 15)

        self.assertGreaterEqual(reset_time, 0)
        self.assertLessEqual(reset_time, 900)

    def test_collector_gets_all_friends_of_power_user(self):

        collector = Collector(self.connection, seed=2343198944)

        user_friends = collector.get_friend_list()

        self.assertGreater(len(user_friends), 5000)


if __name__ == "__main__":
    unittest.main()
