import json
import os

def function_for_testing():
    try:
        key_file = json.load(open(os.path.join("tests","keys.json"), "r"))
    except FileNotFoundError:
        raise FileNotFoundError('"keys.json" could not be found')

    if "consumer_key" not in key_file or "consumer_secret" not in key_file:
        raise KeyError('"keys.json" does not contain the key "consumer_key" or "consumer_secret"')
