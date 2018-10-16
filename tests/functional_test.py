# functional test for network collector

import unittest


class FirstUseTest(unittest.TestCase):
    """Functional test for first use of the program."""

    def test_starts_and_checks_for_necessary_input(self):
        # user starts program with `start.py`
        # There is a key file (json) or error
        # There is a seed file (csv with Twitter IDs) or error
        # There is a config with result-database details or error
        # Program returns number of available keys and tests whether they can connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)

        pass

# TODO: create Hans Bredow SparseTwitter App and OAuth some users
