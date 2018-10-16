# functional test for network collector
import start
import unittest
import json
from json import JSONDecodeError


class FirstUseTest(unittest.TestCase):
    """Functional test for first use of the program."""

    def test_starts_and_checks_for_necessary_input(self):
        # user starts program with `start.py`
        # response = str(check_output('python start.py', stderr=STDOUT,
        # shell=True), encoding="ascii")

        # If there is no key file, a bad key file, or an incomplete key file,
        # there will be an error.
        with self.assertRaises(FileNotFoundError) and self.assertRaises(
          JSONDecodeError) and self.assertRaises(KeyError):
            start.read_key_file()

        # There is a seed file (csv with Twitter IDs) or error
        # self.assertIn('seed file not found', response)

        # Seed file is not empty or error

        # There is a config with result-database details or error
        # Program returns number of available keys and tests whether they can
        # connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)

# TODO: create Hans Bredow SparseTwitter App and OAuth some users


if __name__ == '__main__':
    unittest.main()
