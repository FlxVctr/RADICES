import json
import os

def function_for_testing():
    key_file = json.load(open(os.path.join("tests","keys.json"), "r"))
