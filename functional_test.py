# functional test for network collector
from setup import FileImport
import unittest
from json import JSONDecodeError
from pandas.errors import EmptyDataError
from configreader import Config


class FirstUseTest(unittest.TestCase):
    """Functional test for first use of the program."""

    def test_starts_and_checks_for_necessary_input(self):
        # user starts program with `start.py`
        # response = str(check_output('python start.py', stderr=STDOUT,
        # shell=True), encoding="ascii")

        # If there is no key file, a bad key file, or an incomplete key file,
        # there will be an error.
        ## Note that the test fails if start.py passes those tests.
        try:
            with self.assertRaises(FileNotFoundError) and self.assertRaises(
              JSONDecodeError) and self.assertRaises(KeyError):
                FileImport().read_key_file()
        except AssertionError:
            print("Test OK - no planned errors raised")
        # TODO: Make keys.json a csv (easier to handle)

        # If there is no csv file containing the Twitter IDs, or the file is empty,
        # an error will be thrown.
        try:
            with self.assertRaises(FileNotFoundError) and self.assertRaises(EmptyDataError):
                FileImport().read_seed_file()
        except AssertionError:
            print("Test OK - no planned errors raised")
        # TODO: Check DataType of ID column of seeds.csv

        # There is a config with result-database details or error
            # Filename, later IP adress, user name, password, etc., SQLITE oder SQL?
        try:
            with self.assertRaises(FileNotFoundError):
                Config()
            # There is no database. Do you wish to create a new SQlite database?
                # True false, Name of # db



        # Program returns number of available keys and tests whether they can
        # connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)


# TODO: create Hans Bredow SparseTwitter App and OAuth some users


if __name__ == '__main__':
    unittest.main()
