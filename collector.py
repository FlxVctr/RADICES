import tweepy
import time
import pandas as pd

from setup import FileImport


class Connection(object):
    """Class that handles the connection to Twitter

    Attributes:
        token_file_name (str): Path to file with user tokens
    """

    def __init__(self, token_file_name="tokens.csv"):

        self.credentials = FileImport().read_app_key_file()

        self.tokens = FileImport().read_token_file(token_file_name)

        self.ctoken = self.credentials[0]
        self.csecret = self.credentials[1]

        self.token_number = 0

        self.token = self.tokens['token'][0]
        self.secret = self.tokens['secret'][0]

        self.auth = tweepy.OAuthHandler(self.ctoken, self.csecret)
        self.auth.set_access_token(self.token, self.secret)
        # TODO: implement case if we have more than one token and secret

        self.api = tweepy.API(self.auth)

    def next_token(self):

        if self.token_number < len(self.tokens):
            self.token_number += 1
            self.token = self.tokens['token'][self.token_number]
            self.secret = self.tokens['secret'][self.token_number]
        else:
            self.token_number = 0
            self.token = self.tokens['token'][self.token_number]
            self.secret = self.tokens['secret'][self.token_number]

        self.auth = tweepy.OAuthHandler(self.ctoken, self.csecret)
        self.auth.set_access_token(self.token, self.secret)

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

    def make_friend_df(friends_details, select=[]):
        """Transforms list of user details to pandas.DataFrame

        Args:
            friends_details (list of Tweepy user objects)
            select (list of str): columns to keep in DataFrame

        Returns:
            pandas.DataFrame with these columns or selected as by `select`:
                ['contributors_enabled',
                 'created_at',
                 'default_profile',
                 'default_profile_image',
                 'description',
                 'entities.description.urls',
                 'entities.url.urls',
                 'favourites_count',
                 'follow_request_sent',
                 'followers_count',
                 'following',
                 'friends_count',
                 'geo_enabled',
                 'has_extended_profile',
                 'id',
                 'id_str',
                 'is_translation_enabled',
                 'is_translator',
                 'lang',
                 'listed_count',
                 'location',
                 'name',
                 'needs_phone_verification',
                 'notifications',
                 'profile_background_color',
                 'profile_background_image_url',
                 'profile_background_image_url_https',
                 'profile_background_tile',
                 'profile_banner_url',
                 'profile_image_url',
                 'profile_image_url_https',
                 'profile_link_color',
                 'profile_sidebar_border_color',
                 'profile_sidebar_fill_color',
                 'profile_text_color',
                 'profile_use_background_image',
                 'protected',
                 'screen_name',
                 'status.contributors',
                 'status.coordinates',
                 'status.coordinates.coordinates',
                 'status.coordinates.type',
                 'status.created_at',
                 'status.entities.hashtags',
                 'status.entities.media',
                 'status.entities.symbols',
                 'status.entities.urls',
                 'status.entities.user_mentions',
                 'status.extended_entities.media',
                 'status.favorite_count',
                 'status.favorited',
                 'status.geo',
                 'status.geo.coordinates',
                 'status.geo.type',
                 'status.id',
                 'status.id_str',
                 'status.in_reply_to_screen_name',
                 'status.in_reply_to_status_id',
                 'status.in_reply_to_status_id_str',
                 'status.in_reply_to_user_id',
                 'status.in_reply_to_user_id_str',
                 'status.is_quote_status',
                 'status.lang',
                 'status.place',
                 'status.place.bounding_box.coordinates',
                 'status.place.bounding_box.type',
                 'status.place.contained_within',
                 'status.place.country',
                 'status.place.country_code',
                 'status.place.full_name',
                 'status.place.id',
                 'status.place.name',
                 'status.place.place_type',
                 'status.place.url',
                 'status.possibly_sensitive',
                 'status.quoted_status_id',
                 'status.quoted_status_id_str',
                 'status.retweet_count',
                 'status.retweeted',
                 'status.retweeted_status.contributors',
                 'status.retweeted_status.coordinates',
                 'status.retweeted_status.created_at',
                 'status.retweeted_status.entities.hashtags',
                 'status.retweeted_status.entities.media',
                 'status.retweeted_status.entities.symbols',
                 'status.retweeted_status.entities.urls',
                 'status.retweeted_status.entities.user_mentions',
                 'status.retweeted_status.extended_entities.media',
                 'status.retweeted_status.favorite_count',
                 'status.retweeted_status.favorited',
                 'status.retweeted_status.geo',
                 'status.retweeted_status.id',
                 'status.retweeted_status.id_str',
                 'status.retweeted_status.in_reply_to_screen_name',
                 'status.retweeted_status.in_reply_to_status_id',
                 'status.retweeted_status.in_reply_to_status_id_str',
                 'status.retweeted_status.in_reply_to_user_id',
                 'status.retweeted_status.in_reply_to_user_id_str',
                 'status.retweeted_status.is_quote_status',
                 'status.retweeted_status.lang',
                 'status.retweeted_status.place',
                 'status.retweeted_status.possibly_sensitive',
                 'status.retweeted_status.quoted_status_id',
                 'status.retweeted_status.quoted_status_id_str',
                 'status.retweeted_status.retweet_count',
                 'status.retweeted_status.retweeted',
                 'status.retweeted_status.source',
                 'status.retweeted_status.text',
                 'status.retweeted_status.truncated',
                 'status.source',
                 'status.text',
                 'status.truncated',
                 'statuses_count',
                 'suspended',
                 'time_zone',
                 'translator_type',
                 'url',
                 'verified']
                 'utc_offset',
        """

        json_list = [friend._json for friend in friends_details]

        df = pd.io.json.json_normalize(json_list)

        if select != []:
            df = df[select]

        return df
