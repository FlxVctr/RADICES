import json
from json import JSONDecodeError
import pandas as pd
import yaml


class FileImport():
    def read_app_key_file(self):
        """
        Reads "keys.json" from the main directory and returns the consumer tokens and secrets for
        the Twitter API
        :return: Either two single values or two lists of consumer tokens and secrets for the
        Twitter API
        """

        # TODO: change return to dictionary

        try:
            with open("keys.json", "r") as f:
                self.key_file = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('"keys.json" could not be found')
        except JSONDecodeError as e:
            print('''Bad JSON file. Please check that "keys.json" is formatted
             correctly and that it is not empty''')
            raise e
        if "consumer_token" not in self.key_file or "consumer_secret" not in self.key_file:
            raise KeyError('''"keys.json" does not contain the dictionary keys
             "consumer_token" and/or "consumer_secret"''')

        if type(self.key_file["consumer_secret"]) is not str or type(
                self.key_file["consumer_token"]) is not str:
                    raise TypeError("Consumer secret is type" +
                                    str(type(self.key_file["consumer_secret"])) +
                                    "and consumer token is type " + str(type(
                                     self.key_file["consumer_token"])) + '''. Both
                                     must be of type str. ''')

        return (self.key_file["consumer_token"], self.key_file["consumer_secret"])

    def read_seed_file(self):
        try:
            with open("seeds.csv", "r") as f:
                self.seeds = pd.read_csv(f)
        except FileNotFoundError:
            raise FileNotFoundError('"seeds.csv" could not be found')
        except pd.errors.EmptyDataError as e:
            raise e
        return self.seeds

    def read_token_file(self, filename="tokens.csv"):
        """Reads file with tokens (csv)

        Args:
            filename (str, optional): Defaults to "tokens.csv"

        Returns:
            pandas.DataFrame: With columns `token` and `secret`, one line per user
        """
        return pd.read_csv(filename)


class Config():

    config_path = "config.yml"
    config_template = "config_template.py"

    def __init__(self):
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('''Could not find "config.yml".\n
            Please run "python3 make_config.py" or provide a config.yml''')

        self.mysql_config = self.config["mysql"]
        self.dbtype = self.mysql_config["dbtype"]
        self.dbhost = self.mysql_config["host"]
        self.dbuser = self.mysql_config["user"]
        self.dbpwd = self.mysql_config["passwd"]
        self.dbname = self.mysql_config["dbname"]
