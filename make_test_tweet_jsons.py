import json
import os
from collector import Connection, Collector

# Setup the Collector
seed = 83662933  # Swap seeds with another Twitter User ID if you like
con = Connection()
collector = Collector(con, seed)

# Get the friends and details of the specified seed
friends = collector.get_friend_list()
friends_details = collector.get_details(friends)

# CHeck for the relevant directory
if not os.path.isdir(os.path.join("tests", "tweet_jsons")):
    os.mkdir(os.path.join("tests", "tweet_jsons"))

# Write details in json files
ct = 1
for friend_details in friends_details:
    with open(os.path.join("tests", "tweet_jsons", "user_"+str(ct)+".json"), "w") as f:
        json.dump(friend_details._json, f)
    ct += 1
