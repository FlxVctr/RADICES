# functional test for network collector
import argparse
import pandas as pd
import os
import shutil
import sys
import unittest
import warnings
import yaml
from sqlalchemy.exc import InternalError
from subprocess import PIPE, STDOUT, CalledProcessError, Popen, check_output

import test_helpers
from collector import Coordinator
from database_handler import DataBaseHandler
from exceptions import TestException
from start import main_loop


parser = argparse.ArgumentParser(description='SparseTwitter FunctionalTestSuite')
parser.add_argument('-w', '--show_resource_warnings',
                    help='If set, will show possible resource warnings from the requests package.',
                    required=False,
                    action='store_true')
parser.add_argument('unittest_args', nargs='*')

args = parser.parse_args()
show_warnings = args.show_resource_warnings
sys.argv[1:] = args.unittest_args

mysql_cfg = test_helpers.config_dict_user_details_dtypes_mysql


def setUpModule():
    if not show_warnings:
        warnings.filterwarnings(action="ignore",
                                message="unclosed",
                                category=ResourceWarning)


class FirstUseTest(unittest.TestCase):

    """Functional test for first use of the program."""

    @classmethod
    def setUpClass(cls):
        os.rename("seeds.csv", "seeds.csv.bak")
        if os.path.exists("latest_seeds.csv"):
            os.rename("latest_seeds.csv", "latest_seeds.csv.bak")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("seeds.csv"):
            os.remove("seeds.csv")
        os.rename("seeds.csv.bak", "seeds.csv")
        if os.path.exists("latest_seeds.csv.bak"):
            os.rename("latest_seeds.csv.bak", "latest_seeds.csv")

    def setUp(self):
        if os.path.isfile("config.yml"):
            os.rename("config.yml", "config.yml.bak")

    def tearDown(self):
        if os.path.isfile("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")
        if os.path.isfile("seeds.csv"):
            os.remove("seeds.csv")

        dbh = DataBaseHandler(config_dict=mysql_cfg, create_all=False)

        try:
            dbh.engine.execute("DROP TABLE friends")
        except InternalError:
            pass
        try:
            dbh.engine.execute("DROP TABLE user_details")
        except InternalError:
            pass
        try:
            dbh.engine.execute("DROP TABLE result")
        except InternalError:
            pass

    def test_starts_and_checks_for_necessary_input_seeds_missing(self):
        if os.path.isfile("seeds.csv"):
            os.remove("seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mysql_cfg, f, default_flow_style=False)

        # User starts program with `start.py`
        try:
            response = str(check_output('python start.py', stderr=STDOUT,
                                        shell=True), encoding="ascii")

        # ... and encounters an error because the seeds.csv is missing.
        except CalledProcessError as e:
            response = str(e.output)
            self.assertIn('"seeds.csv" could not be found', response)

    def test_starts_and_checks_for_necessary_input_seeds_empty(self):
        # User starts program with `start.py`
        shutil.copyfile("seeds_empty.csv", "seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mysql_cfg, f, default_flow_style=False)

        try:
            response = str(check_output('python start.py', stderr=STDOUT,
                                        shell=True), encoding="ascii")

        # ... and encounters an error because the seeds.csv is empty.
        except CalledProcessError as e:
            response = str(e.output)
            self.assertIn('"seeds.csv" is empty', response)

    def test_starts_and_checks_for_necessary_input_config_missing(self):
        # user starts program with `start.py`
        if not os.path.exists("seeds.csv"):
            shutil.copyfile("seeds.csv.bak", "seeds.csv")
        try:
            response = str(check_output('python start.py', stderr=STDOUT,
                                        shell=True), encoding="ascii")

        # ... and encounters an error because:
        except CalledProcessError as e:
            response = str(e.output)
            # ... the config.yml is missing. Ergo the user creates a new one using make_config.py
            self.assertIn("provide a config.yml", response)
            if "provide a config.yml" in response:
                # Does make_config.py not make a new config.yml when entered "n"?
                p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE,
                          shell=True)
                p.communicate("n\n".encode())
                self.assertFalse(os.path.isfile("config.yml"))

                # Does make_config.py open a dialogue asking to open the new config.yaml?
                p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE,
                          shell=True)
                p.communicate("y\n".encode())

            self.assertTrue(os.path.exists("config.yml"))

            with open("config.yml", "w") as f:
                yaml.dump(mysql_cfg, f, default_flow_style=False)

            DataBaseHandler().engine.execute("DROP TABLES friends, user_details, result;")

    def test_starting_collectors_and_writing_to_db(self):

        shutil.copyfile("seeds_test.csv", "seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mysql_cfg, f, default_flow_style=False)

        try:
            response = str(check_output('python start.py -n 2 -l de -t',
                                        stderr=STDOUT, shell=True))
            print(response)
        except CalledProcessError as e:
            response = str(e.output)
            print(response)
            raise e

        dbh = DataBaseHandler()

        result = pd.read_sql("result", dbh.engine)

        self.assertLessEqual(len(result), 8)

        self.assertNotIn(True, result.duplicated().values)

        dbh.engine.execute("DROP TABLE friends, user_details, result;")

    def test_restarts_after_exception(self):

        shutil.copyfile("two_seeds.csv", "seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mysql_cfg, f, default_flow_style=False)

        with self.assertRaises(TestException):
            main_loop(Coordinator(), test_fail=True)

        p = Popen("python start.py -n 2 -t -f", stdout=PIPE, stderr=PIPE, stdin=PIPE,
                  shell=True)

        stdout, stderr = p.communicate()

        self.assertIn("Retrying", str(stdout))  # tries to restart itself

        latest_seeds = set(pd.read_csv("latest_seeds.csv", header=None)[0].values)
        seeds = set(pd.read_csv('seeds.csv', header=None)[0].values)

        self.assertEqual(latest_seeds, seeds)

        q = Popen("python start.py -t --restart", stdout=PIPE, stderr=PIPE, stdin=PIPE,
                  shell=True)

        stdout, stderr = q.communicate()

        self.assertIn("Restarting with latest seeds:", stdout.decode('utf-8'),
                      msg=f"{stdout.decode('utf-8')}\n{stderr.decode('utf-8')}")

        latest_seeds = set(pd.read_csv("latest_seeds.csv", header=None)[0].values)

        self.assertNotEqual(latest_seeds, seeds)

        DataBaseHandler().engine.execute("DROP TABLE friends, user_details, result;")

    def test_collects_only_requested_number_of_pages_of_friends(self):

        shutil.copyfile("seed_with_lots_of_friends.csv", "seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mysql_cfg, f, default_flow_style=False)

        try:
            response = str(check_output('python start.py -n 1 -t -p 1',
                                        stderr=STDOUT, shell=True))
            print(response)
        except CalledProcessError as e:
            response = str(e.output)
            print(response)
            raise e

        dbh = DataBaseHandler()

        result = pd.read_sql("SELECT COUNT(*) FROM friends WHERE source = 2343198944", dbh.engine)

        result = result['COUNT(*)'][0]

        self.assertLessEqual(result, 5000)
        self.assertGreater(result, 4000)

        dbh.engine.execute("DROP TABLE friends, user_details, result;")


if __name__ == '__main__':
    unittest.main()
