# functional test for network collector
import copy
import os
import shutil
import unittest
from subprocess import PIPE, STDOUT, CalledProcessError, Popen, check_output

import pandas as pd
import yaml
from sqlalchemy.exc import InternalError

import passwords
import test_helpers
from collector import Coordinator
from database_handler import DataBaseHandler
from start import TestException, main_loop

config_dict = test_helpers.config_dict
mock_sql_cfg = copy.deepcopy(config_dict)
mock_sql_cfg["sql"] = dict(
    dbtype='mysql',
    host='127.0.0.1',
    user='sparsetwitter',
    passwd=passwords.sparsetwittermysqlpw,
    dbname="sparsetwitter"
)


class FirstUseTest(unittest.TestCase):

    """Functional test for first use of the program."""

    @classmethod
    def setUpClass(cls):
        os.rename("seeds.csv", "seeds.csv.bak")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("seeds.csv"):
            os.remove("seeds.csv")
        os.rename("seeds.csv.bak", "seeds.csv")

    def setUp(self):
        if os.path.isfile("config.yml"):
            os.rename("config.yml", "config.yml.bak")

    def tearDown(self):
        if os.path.isfile("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")
        if os.path.isfile("seeds.csv"):
            os.remove("seeds.csv")

        dbh = DataBaseHandler()

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
            yaml.dump(mock_sql_cfg, f, default_flow_style=False)

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
            yaml.dump(mock_sql_cfg, f, default_flow_style=False)

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
                yaml.dump(mock_sql_cfg, f, default_flow_style=False)

            DataBaseHandler().engine.execute("DROP TABLES friends, user_details, result;")

    def test_starting_collectors_and_writing_to_db(self):

        shutil.copyfile("seeds_test.csv", "seeds.csv")

        with open("config.yml", "w") as f:
            yaml.dump(mock_sql_cfg, f, default_flow_style=False)

        try:
            response = str(check_output('python start.py -n 2 -l de -t',
                                        stderr=STDOUT, shell=True),
                           encoding="ascii")
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
            yaml.dump(mock_sql_cfg, f, default_flow_style=False)

        with self.assertRaises(TestException):
            main_loop(Coordinator(), test_fail=True)

        p = Popen("python start.py -n 2 -t -f", stdout=PIPE, stderr=PIPE, stdin=PIPE,
                  shell=True)

        stdout, stderr = p.communicate()

        self.assertIn("Retrying", str(stdout))

        latest_seeds = set(pd.read_csv("latest_seeds.csv", header=None)[0].values)
        seeds = set(pd.read_csv('seeds.csv', header=None)[0].values)

        self.assertEqual(latest_seeds, seeds)

        DataBaseHandler().engine.execute("DROP TABLE friends, user_details, result;")


if __name__ == '__main__':
    unittest.main()
