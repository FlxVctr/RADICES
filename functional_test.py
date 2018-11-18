# functional test for network collector
import copy
import os
import passwords
import test_helpers
import unittest
import yaml
from database_handler import DataBaseHandler
from subprocess import check_output, STDOUT, CalledProcessError, Popen, PIPE

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
        if os.path.exists("config.yml"):
            os.rename("config.yml", "config.yml.bak")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("config.yml.bak"):
            os.replace("config.yml.bak", "config.yml")

    def test_starts_and_checks_for_necessary_input(self):
        # If make_config.py was run
        if os.path.exists("config.yml"):
            with open('config.yml', 'w') as f:
                yaml.dump(mock_sql_cfg, f, default_flow_style=False)

        # user starts program with `start.py`
        try:
            response = str(check_output('python start.py', stderr=STDOUT,
                           shell=True), encoding="ascii")

        # ... and encounters an error because:
        except CalledProcessError as e:
            response = str(e.output)
        # ... the config.yml is missing. Ergo the user creates a new one using make_config.py
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

                    # The user executes start.py again
                    self.test_starts_and_checks_for_necessary_input()

        except Exception:
            self.fail("Failed on error")

        print(response)

        DataBaseHandler().engine.execute("DROP TABLES friends, user_details;")
        self.fail("Failes without Error!")
        # program looks for an existing twitter key file and imports the keys
        # program looks for an existing seeds file and imports the seeds

        # There is a config with result-database details or error
        # (Filename, later IP adress, user name, password, etc., SQLITE oder SQL?)

        # There is no database. Do you wish to create a new SQlite database?
        # True false, Name of # db

        # Program returns number of available keys and tests whether they can
        # connect
        # Program returns number of seeds
        # Program tests connection to database (MySQL or BigQuery?)

    # User does not have a config.yml so she executes make_config.py
    def make_a_new_config_yml(self):
        # Does make_config.py not make a new config.yml when entered "n"?
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=True)
        p.communicate("n\n".encode())
        self.assertFalse(os.path.isfile("config.yml"))

        # Does make_config.py open a dialogue asking to open the new config.yaml?
        # (Just close the dialogue)
        p = Popen("python make_config.py", stdout=PIPE, stderr=PIPE, stdin=PIPE, shell=True)
        p.communicate("y\n".encode())


if __name__ == '__main__':
    unittest.main()
