import json
from json import JSONDecodeError
import pandas as pd


class FileImport():
    def read_key_file(self):
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
            print('''BAD JSON FILE. PLEASE CHECK IF "keys.json" IS FORMATTED
             CORRECTLY''')
            raise e
        if "consumer_token" not in self.key_file or "consumer_secret" not in self.key_file:
            raise KeyError('''"keys.json" does not contain the dictionary keys
             "consumer_token" and/or "consumer_secret"''')

        if len(self.key_file["consumer_token"]) != len(self.key_file["consumer_secret"]):
            raise Exception("Number of consumer tokens does not match number of consumer secrets")

        return self.key_file["consumer_token"], self.key_file["consumer_secret"]

    def read_seed_file(self):
        try:
            with open("seeds.csv", "r") as f:
                self.seeds = pd.read_csv(f)
        except FileNotFoundError:
            raise FileNotFoundError('"seeds.csv" could not be found')
        except pd.errors.EmptyDataError as e:
            raise e
        return self.seeds
