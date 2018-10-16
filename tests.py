from make_db import DataBaseHandler
import unittest
import os


class DatabaseHandlerTest(unittest.TestCase):

    db_name = "NiceDB"

    #def tearDown(self):
    #    os.remove(self.db_name + ".db")

    def test_no_database_(self):
        DataBaseHandler()

    def test_database_handler_creates_database_from_given_name(self, db_name=db_name):

        dbh = DataBaseHandler()
        dbh.new_db(db_name=db_name)
        try:
            self.assertTrue(os.path.isfile(self.db_name + ".db"))
        except AssertionError:
            print("Database was not created!")


if __name__ == "__main__":
    unittest.main()
