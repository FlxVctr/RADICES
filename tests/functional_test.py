# functional test for network collector
import start
import unittest
import os
from subprocess import check_output, STDOUT

class FirstUseTest(unittest.TestCase):
    """Functional test for first use of the program."""

    def test_starts_and_checks_for_necessary_input(self):
        # user starts program with `start.py`
        #  response = str(check_output('python start.py', stderr=STDOUT, shell=True), encoding="ascii")

        self.assertRaises(FileNotFoundError, start.function_for_testing())

        # There is a key file (json) or error
        self.assertIn('keys.json not found', response)

        # key file may not be empty
        self.assertIn('key file empty', response)

        # There is a seed file (csv with Twitter IDs) or error
        self.assertIn('seed file not found', response)

        # Seed file is not empty or error

        # There is a config with result-database details or error
        # Program returns number of available keys and tests whether they can connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)

# TODO: create Hans Bredow SparseTwitter App and OAuth some users


if __name__ == '__main__':
    unittest.main()
