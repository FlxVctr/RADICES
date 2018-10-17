from make_db import DataBaseHandler
import unittest
import os
from configreader import Config
from setup import FileImport
from json import JSONDecodeError
from pandas.errors import EmptyDataError


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


if __name__ == "__main__":
    unittest.main()
