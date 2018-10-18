import tweepy

from setup import FileImport


class Connection(object):
    """Class that handles the connection to Twitter

    Attributes:
        token_file_name (str): Path to file with user tokens
    """

    def __init__(self, token_file_name="tokens.csv"):

        self.credentials = FileImport().read_app_key_file()

        self.tokens = FileImport().read_token_file(token_file_name)

        ctoken = self.credentials[0]
        csecret = self.credentials[1]

        token = self.tokens['token'][0]
        secret = self.tokens['secret'][0]

        self.auth = tweepy.OAuthHandler(ctoken, csecret)
        self.auth.set_access_token(token, secret)
        # TODO: implement case if we have more than one token and secret

        self.api = tweepy.API(self.auth)
