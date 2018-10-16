import json
from json import JSONDecodeError
import os


def read_key_file():
    try:
        key_file = json.load(open(os.path.join("keys.json"), "r"))
    except FileNotFoundError:
        raise FileNotFoundError('"keys.json" could not be found')
    except JSONDecodeError as e:
        print('BAD JSON FILE. PLEASE CHECK IF "keys.json" IS FORMATTED CORRECTLY')
        raise e
    if "consumer_key" not in key_file or "consumer_secret" not in key_file:
        raise KeyError('"keys.json" does not contain the dictionary keys "consumer_key" and/or "consumer_secret"')
