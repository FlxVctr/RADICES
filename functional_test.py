# functional test for network collector
import unittest
from configreader import Config
from subprocess import check_output, STDOUT, CalledProcessError


class FirstUseTest(unittest.TestCase):
    """Functional test for first use of the program."""

    def test_starts_and_checks_for_necessary_input(self):
        # user starts program with `start.py`
        try:
            response = str(check_output('python start.py', stderr=STDOUT,
                           shell=True), encoding="ascii")

        # If there is no key file, a bad key file, or an incomplete key file,
        # there will be an error.

        # If there is no csv file containing the Twitter IDs, or the file is empty,
        # an error will be thrown.

        # There is a config with result-database details or error
        # (Filename, later IP adress, user name, password, etc., SQLITE oder SQL?)

            with self.assertRaises(FileNotFoundError):
                Config()
            # There is no database. Do you wish to create a new SQlite database?
            # True false, Name of # db

        # Program returns number of available keys and tests whether they can
        # connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)

        # TODO: create Hans Bredow SparseTwitter App and OAuth some users
        except CalledProcessError:
            self.fail("Failed on error")


if __name__ == '__main__':
    unittest.main()
