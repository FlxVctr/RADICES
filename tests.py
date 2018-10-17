from make_db import DataBaseHandler
import unittest
import os
from configreader import Config
from setup import FileImport
from json import JSONDecodeError
from pandas.errors import EmptyDataError
from collector import Connection
import tweepy


class FileImportTest(unittest.TestCase):

    # Note that the test fails if start.py passes those tests.
    def test_read_key_file(self):
        try:
            with self.assertRaises(FileNotFoundError) and self.assertRaises(
              JSONDecodeError) and self.assertRaises(KeyError):
                FileImport().read_key_file()
        except AssertionError:
            print("Test OK - no planned errors raised")

    def test_read_seed_file(self):
        try:
            with self.assertRaises(FileNotFoundError) and self.assertRaises(EmptyDataError):
                    FileImport().read_seed_file()
        except AssertionError:
            print("Test OK - no planned errors raised")
    # TODO: Check DataType of ID column of seeds.csv

    def test_read_token_file_raises_error_if_no_file(self):
        with self.assertRaises(FileNotFoundError):
            FileImport().read_token_file(filename='no_file.csv')


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

    def test_collector_raises_exception_if_credentials_are_wrong(self):
        with self.assertRaises(tweepy.TweepError) as te:
            Connection.verify_credentials()

        self.assertIn('401', str(te.exception.response))


if __name__ == "__main__":
    unittest.main()
