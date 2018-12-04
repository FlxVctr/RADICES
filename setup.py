import json
from json import JSONDecodeError
import pandas as pd
import yaml


class FileImport():
    def read_app_key_file(self, filename: str = "keys.json") -> tuple:
        """Reads file with consumer key and consumer secret (JSON)

        Args:
            filename (str, optional): Defaults to "keys.json"

        Returns:
            Tuple with two strings: (1) being the twitter consumer token and (2) being the
            twitter consumer secret
        """

        # TODO: change return to dictionary

        try:
            with open(filename, "r") as f:
                self.key_file = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('"keys.json" could not be found')
        except JSONDecodeError as e:
            print("Bad JSON file. Please check that 'keys.json' is formatted\
                  correctly and that it is not empty")
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

    def read_seed_file(self, filename: str = "seeds.csv") -> pd.DataFrame:
        """Reads file with consumer key and consumer secret (JSON)

        Args:
            filename (str, optional): Defaults to "seeds.csv"

        Returns:
            A single column pandas DataFrame with one Twitter ID (seed) each row.
        """
        try:
            with open("seeds.csv", "r") as f:
                self.seeds = pd.read_csv(f, header=None)
        except FileNotFoundError:
            raise FileNotFoundError('"seeds.csv" could not be found')
        except pd.errors.EmptyDataError as e:
            print('"seeds.csv" is empty!')
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
    """Class that handles the SQL and twitter user details configuration.

    Attributes:
        config_file (str): Path to configuration file
        config_dict (dict): Dictionary containing the config information (in case
                            the dictionary shall be directly passed instead of read
                            out of a configuration file).
    """
    config_template = "config_template.py"

    def __init__(self, config_file="config.yml", config_dict: dict=None):
        if config_dict is not None:
            self.config = config_dict
        else:
            self.config_path = config_file
            try:
                with open(self.config_path, 'r') as f:
                    self.config = yaml.load(f)
            except FileNotFoundError:
                raise FileNotFoundError('Could not find "' + self.config_path + '''".\n
                Please run "python3 make_config.py" or provide a config.yml''')

        if "sql" not in self.config:
            print("Config file " + config_file + """ does not contain key 'sql'!
                  Will use default sqlite configuration.""")
            self.config["sql"] = dict(dbtype="sqlite",
                                      dbname="new_database")
        self.sql_config = self.config["sql"]

        # No db type given in Config
        if self.sql_config["dbtype"] is None:
            print('''Parameter dbtype not set in the "config.yml". Will create
                             an sqlite database.''')
            self.dbtype = "sqlite"
        else:
            self.dbtype = self.sql_config["dbtype"].strip()

        # DB type is SQL - checking for all parameters
        if self.dbtype == "mysql":
            try:
                self.dbhost = str(self.sql_config["host"])
                self.dbuser = str(self.sql_config["user"])
                self.dbpwd = str(self.sql_config["passwd"])
                if self.dbhost == '':
                    raise ValueError("dbhost parameter is empty")
                if self.dbuser == '':
                    raise ValueError("dbuser parameter is empty")
                if self.dbpwd == '':
                    raise ValueError("passwd parameter is empty")
            except KeyError as e:
                raise e
        elif self.dbtype == "sqlite":
            self.dbhost = None
            self.dbuser = None
            self.dbpwd = None
        else:
            raise ValueError('''dbtype parameter is neither "sqlite" nor
                                      "mysql". Please adjust the "config.yml" ''')

        # Set db name
        if self.sql_config["dbname"] is not None:
            self.dbname = self.sql_config["dbname"]
        else:
            print('''Parameter "dbname" is missing. New database will have the name
                  "new_database".''')
            self.dbname = "new_database"

        # TODO: Add mailgun config
