from database_handler import DataBaseHandler
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
from twauth import OAuthorizer
import yaml
import shutil
from subprocess import Popen, PIPE
import numpy as np


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

    db_name = Config().dbname

    def tearDown(self):
        if os.path.isfile(self.db_name + ".db"):
            os.remove(self.db_name + ".db")

    def test_setup_and_database_handler_creates_database_from_given_name(self, db_name=db_name):
        DataBaseHandler()
        try:
            self.assertTrue(os.path.isfile(self.db_name + ".db"))
        except AssertionError:
            print("Database was not created!")


class OAuthTest(unittest.TestCase):
    def setUp(self):
        if os.path.isfile("keys.json"):
            os.rename("keys.json", "keys_bak.json")
        if os.path.isfile("empty_keys.json"):
            os.rename("empty_keys.json", "keys.json")

    def tearDown(self):
        if os.path.isfile("keys.json"):
            os.rename("keys.json", "empty_keys.json")
        if os.path.isfile("keys_bak.json"):
            os.replace("keys_bak.json", "keys.json")

    def test_oauth_throws_error_when_request_token_not_get(self):
        with self.assertRaises(tweepy.TweepError):
            OAuthorizer()

    def test_wrong_verifier_entered(self):
        pass

    def test_new_line_in_csv_after_verifying(self):
        pass


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

    def test_collector_gets_friend_details_and_makes_df(self):

        collector = Collector(self.connection, seed=36476777)

        user_friends = collector.get_friend_list()
        friends_details = collector.get_details(user_friends)

        self.assertGreaterEqual(len(friends_details), 100)

        friends_df = Collector.make_friend_df(friends_details)

        self.assertIsInstance(friends_df, pd.DataFrame)
        self.assertEqual(len(friends_df), len(friends_details))

        self.assertIsInstance(friends_df['id'][0], np.int64)
        self.assertIsInstance(friends_df['screen_name'][0], str)
        self.assertIsInstance(friends_df['friends_count'][0], np.int64)

        friends_df_selected = Collector.make_friend_df(friends_details,
                                                       select=['id', 'followers_count',
                                                               'created_at'])

        self.assertEqual(len(friends_df_selected.columns), 3)
        self.assertIsInstance(friends_df['id'][0], np.int64)
        self.assertIsInstance(friends_df['created_at'][0], str)
        self.assertIsInstance(friends_df['followers_count'][0], np.int64)

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
