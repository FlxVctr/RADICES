from make_db import DataBaseHandler
import unittest
import os.path


class DatabaseHandlerTest(unittest.TestCase):

    def test_no_database_(self):
        DataBaseHandler()

    def test_database_handler_creates_database_from_given_name(self):
        self.db_name = "NiceDB"
        dbh = DataBaseHandler()
        dbh.new_db(db_name=self.db_name)
        try:
            self.assertTrue(os.path.isfile(self.db_name + ".db"))
        except: "Database was not created!"


if __name__ == "__main__":
    unittest.main()
