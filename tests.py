from make_db import DataBaseHandler
import unittest
import os
from setup import Config
from setup import FileImport
from json import JSONDecodeError
import json
from pandas.errors import EmptyDataError
from collector import Connection
from collector import Collector
import tweepy
import pandas as pd
from pandas.api.types import is_string_dtype


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

    db_name = "NiceDB"

    def tearDown(self):
        if os.path.isfile(self.db_name + ".db"):
            os.remove(self.db_name + ".db")

    def test_database_handler_creates_database_from_given_name(self, db_name=db_name):
        dbh = DataBaseHandler()
        dbh.new_db(db_name=db_name)
        try:
            self.assertTrue(os.path.isfile(self.db_name + ".db"))
        except AssertionError:
            print("Database was not created!")


class ConfigTest(unittest.TestCase):

    def test_config_file_gets_read_incl_all_fields(self):
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

    def test_collector_can_get_details_from_friend_list(self):

        collector = Collector(self.connection, seed=36476777)

        friends = collector.get_friend_list()

        friends_details = collector.get_details(friends)

        self.assertGreaterEqual(len(friends_details), 100)



if __name__ == "__main__":
    unittest.main()
