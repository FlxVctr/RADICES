import argparse
import json
import os
from collector import Connection, Collector
from tweepy.error import TweepError

parser = argparse.ArgumentParser(description='SparseTwitter user_details Downloader')
parser.add_argument('-s', '--seed',
                    help='''Provide a seed (=Twitter user ID). Its friends details will
                         be downloaded.''',
                    required=False,
                    type=int,
                    default=83662933)

# Setup the Collector
seed = parser.parse_args().seed  # Swap seeds with another Twitter User ID if you like
if seed == 83662933:
    print("No seed given. Using default seed " + str(seed) + ".")
else:
    print("Downloading and saving friends' details of user " + str(seed) + ".")
con = Connection()
collector = Collector(con, seed)

# Get the friends and details of the specified seed
try:
    friends = collector.get_friend_list()
except TweepError as e:
    if "'code': 34" in e.reason:
        raise TweepError("The seed you have given is not a valid Twitter user ID")
friends_details = collector.get_details(friends)

# Check for the relevant directory
if not os.path.isdir(os.path.join("tests", "tweet_jsons")):
    os.mkdir(os.path.join("tests", "tweet_jsons"))

# Write details in json files
ct = 1
for friend_details in friends_details:
    with open(os.path.join("tests", "tweet_jsons", "user_"+str(ct)+".json"), "w") as f:
        json.dump(friend_details._json, f)
    ct += 1
