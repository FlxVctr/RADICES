from make_db import DataBaseHandler
import unittest
import os
from configreader import Config


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
            'dbtype': None,
            'host': None,
            'user': None,
            'passwd': None,
            'db_name': 'test_db'
            }
        }
        self.assertEqual(Config().config, config_dict)


if __name__ == "__main__":
    unittest.main()
