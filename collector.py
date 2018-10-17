import pandas as pd
import tweepy

from setup import FileImport


class Connection(object):
    """Class that handles the connection to Twitter
    """

    def __init__(self):
        self.credentials = FileImport.read_key_file()
        if type(self.credentials[0]) != list:
            self.auth = tweepy.OAuthHandler(self.credentials[0], self.credentials[1])

    def verify_credentials():
        """Connects to Twitter API to verify that credentials are working
        """
        raise tweepy.TweepError(reason="")
