import tweepy
import time

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

    def remaining_calls(self, endpoint='/friends/ids'):
        """Returns the number of remaining calls until reset time.

        Args:
            endpoint (str):
                API endpoint.
                Defaults to '/friends/ids'
        Returns:
            remaining calls (int)
        """

        rate_limits = self.api.rate_limit_status()

        path = endpoint.split('/')

        path = path[1:]

        rate_limits = rate_limits['resources'][path[0]]

        key = "/" + path[0]

        for item in path[1:]:
            key = key + '/' + item
            rate_limits = rate_limits[key]

        rate_limits = rate_limits['remaining']

        return rate_limits

    def reset_time(self, endpoint='/friends/ids'):
        """Returns the time until reset time.

        Args:
            endpoint (str):
                API endpoint.
                Defaults to '/friends/ids'
        Returns:
            remaining time in seconds (int)
        """

        reset_time = self.api.rate_limit_status()

        path = endpoint.split('/')

        path = path[1:]

        reset_time = reset_time['resources'][path[0]]

        key = "/" + path[0]

        for item in path[1:]:
            key = key + '/' + item
            reset_time = reset_time[key]

        reset_time = reset_time['reset'] - int(time.time())

        print('reset_time = ', reset_time)

        return reset_time


class Collector(object):
    """Does the collecting of friends.

    Attributes:
        connection (Connection object):
            Connection object with actually active credentials
        seed (int): Twitter id of seed user

    """

    def __init__(self, connection, seed):
        self.seed = seed
        self.connection = connection

    def get_friend_list(self, twitter_id=None):
        """Gets the friend list of an account.

        Args:
            twitter_id (int): Twitter Id of account,
                if None defaults to seed account of Collector object.

        Returns:
            list with friends of user.
        """

        if twitter_id is None:
            twitter_id = self.seed

        result = []

        for page in tweepy.Cursor(self.connection.api.friends_ids, user_id=twitter_id).pages():
            result = result + page

        return result

    def get_details(self, friends):
        """Collects details from friends of an account.

        Args:
            friends (list of int): list of Twitter user ids

        Returns:
            list of Tweepy user objects
        """

        i = 0

        user_details = []

        while i < len(friends):

            if i + 100 <= len(friends):
                j = i + 100
            else:
                j = len(friends)
            user_details += self.connection.api.lookup_users(user_ids=friends[i:j])
            i += 100

        return user_details
