import multiprocessing as mp
import time
from sys import stdout

import pandas as pd
import tweepy
from sqlalchemy.exc import ProgrammingError

from database_handler import DataBaseHandler
from setup import FileImport


class Connection(object):
    """Class that handles the connection to Twitter

    Attributes:
        token_file_name (str): Path to file with user tokens
    """

    def __init__(self, token_file_name="tokens.csv", token_queue=None):

        self.credentials = FileImport().read_app_key_file()

        self.ctoken = self.credentials[0]
        self.csecret = self.credentials[1]

        if token_queue is None:
            self.tokens = FileImport().read_token_file(token_file_name)

            self.token_queue = mp.Queue()

            for token in self.tokens.values:
                self.token_queue.put(token)
        else:
            self.token_queue = token_queue

        self.token, self.secret = self.token_queue.get()

        self.auth = tweepy.OAuthHandler(self.ctoken, self.csecret)
        self.auth.set_access_token(self.token, self.secret)
        # TODO: implement case if we have more than one token and secret

        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def next_token(self):

        self.token_queue.put((self.token, self.secret))
        self.token, self.secret = self.token_queue.get()

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

    def check_API_calls_and_update_if_necessary(self, endpoint):
        """Checks for an endpoint how many calls are left and updates token if necessary.

        It iterates through available tokens until one has > 0 calls to `endpoint`. If\
        none is available it waits the minimal reset time it has encountered to check again.

        Args:
            endpoint (str): API endpoint, e.g. '/friends/ids'
        Returns:
            remaining_calls (int)
        """

        remaining_calls = self.connection.remaining_calls(endpoint=endpoint)
        reset_time = self.connection.reset_time(endpoint=endpoint)
        attempts = 0
        first_token = self.connection.token

        while remaining_calls == 0:
            attempts += 1
            stdout.write("Attempt with next token: {}\n".format(attempts))

            self.connection.next_token()

            if self.connection.token == first_token:  # tried all tokens
                msg = "API calls for {e} depleted. Waiting {s} seconds.\n"
                stdout.write(msg.format(e=endpoint, s=reset_time))
                stdout.flush()
                time.sleep(reset_time)
                attempts = 0

            remaining_calls = self.connection.remaining_calls(endpoint=endpoint)
            reset_time = min(reset_time, self.connection.reset_time(endpoint=endpoint))

        return remaining_calls

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

        remaining_calls = self.check_API_calls_and_update_if_necessary(endpoint='/friends/ids')

        for page in tweepy.Cursor(self.connection.api.friends_ids, user_id=twitter_id).pages():
            result = result + page

            remaining_calls -= 1

            if remaining_calls == 0:
                remaining_calls = self.check_API_calls_and_update_if_necessary(
                    endpoint='/friends/ids')

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

        remaining_calls = self.check_API_calls_and_update_if_necessary(endpoint='/friends/ids')

        while i < len(friends):

            if i + 100 <= len(friends):
                j = i + 100
            else:
                j = len(friends)

            if remaining_calls == 0:
                remaining_calls = self.check_API_calls_and_update_if_necessary(
                    endpoint='/users/lookup')

            user_details += self.connection.api.lookup_users(user_ids=friends[i:j])
            i += 100

            remaining_calls -= 1

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

    def check_follows(self, source, target):
        """Checks Twitter API whether `source` account follows `target` account.

        Args:
            source (int): user id
            target (int): user id
        Returns:
            - `True` if `source` follows `target`
            - `False` if `source` does not follow `target`
        """

        friendship = self.connection.api.show_friendship(
            source_id=source, target_id=target)

        following = friendship[0].following

        return following


class Coordinator(object):
    """Selects a queue of seeds and coordinates the collection with collectors
    and a queue of tokens.
    """

    def __init__(self, seeds=2, token_file_name="tokens.csv"):

        self.number_of_seeds = seeds

        self.seed_pool = pd.read_csv("seeds.csv", header=None)

        self.seeds = self.seed_pool.sample(n=self.number_of_seeds)

        self.seeds = self.seeds[0].values

        self.seed_queue = mp.Queue()

        for seed in self.seeds:
            self.seed_queue.put(seed)

        self.tokens = FileImport().read_token_file(token_file_name)

        self.token_queue = mp.Queue()

        for token in self.tokens.values:
            self.token_queue.put(token)

        self.dbh = DataBaseHandler()

    def lookup_accounts_friend_details(self,
                                       account_id, db_connection=None, select="*"):
        """Looks up and retrieves details from friends of `account_id` via database.

        Args:
            account_id (int)
            db_connection (database connection/engine object)
            select (str): comma separated list of required fields, defaults to all available ("*")
        Returns:
            None, if nothing found.
            Otherwise DataFrame with all details.
        """

        if db_connection is None:
            db_connection = self.dbh.engine

        query = "SELECT target from friends WHERE source = {} AND burned = 0".format(account_id)
        friends = pd.read_sql(query, db_connection)

        if len(friends) == 0:
            return None
        else:
            friends = friends['target'].values
            friends = tuple(friends)

            query = "SELECT {} from user_details WHERE id IN {}".format(select, friends)
            friend_detail = pd.read_sql(query, db_connection)

            return friend_detail

    def work_through_seed_get_next_seed(self, seed, select=[], lang=None, connection=None):
        """Takes a seed and determines the next seed and saves all details collected to db.

        Args:
            seed (int)
            select (list of str): fields to save to database, defaults to all
            lang (str): Twitter language code for interface language to filter for,
                defaults to None
            connection (collector.Connection object)
        Returns:
            seed (int)
        """

        if connection is None:
            connection = Connection()
        else:
            connection = connection

        friends_details = None

        try:

            friends_details = self.lookup_accounts_friend_details(
                seed, self.dbh.engine)

        except ProgrammingError:

            print("""Accessing db for friends_details failed. Maybe database does not exist yet.
Accessing Twitter API.""")

        if friends_details is None:

            collector = Collector(connection, seed)

            friend_list = collector.get_friend_list()

            if friend_list == []:  # if account follows nobody
                new_seed = self.seed_pool.sample(n=1)
                new_seed = new_seed[0].values[0]

                stdout.write("No friends or unburned connections left, selecting random seed.\n")

                return new_seed

            self.dbh.write_friends(seed, friend_list)

            friends_details = collector.get_details(friend_list)
            select = select + ["id", "followers_count", "lang", "created_at", "statuses_count"]
            friends_details = Collector.make_friend_df(friends_details, select)

            if lang is not None:
                friends_details = friends_details[friends_details['lang'] == lang]

            friends_details.to_sql('user_details', if_exists='append',
                                   index=False, con=self.dbh.engine)

        max_follower_count = friends_details['followers_count'].max()

        new_seed = friends_details[friends_details['followers_count']
                                   == max_follower_count]['id'].values[0]

        check_exists_query = """
                                SELECT EXISTS(
                                    SELECT * FROM friends
                                    WHERE source={source}
                                    )
                             """.format(source=new_seed)
        node_exists_as_source = self.dbh.engine.execute(check_exists_query).scalar()

        if node_exists_as_source == 1:
            check_follow_query = """
                                    SELECT EXISTS(
                                        SELECT * FROM friends
                                        WHERE source={source} and target={target}
                                        )
                                 """.format(source=new_seed, target=seed)

            follows = self.dbh.engine.execute(check_follow_query).scalar()

        elif node_exists_as_source == 0:
            # check on Twitter

            # FIXTHIS: dirty workaround because of wacky test
            if connection == "fail":
                connection = Connection()

            try:
                collector
            except NameError:
                collector = Collector(connection, seed)

            follows = int(collector.check_follows(source=new_seed, target=seed))

        if follows == 0:
            result = pd.DataFrame({'source': [seed], 'target': [new_seed]})
            result.to_sql('result', if_exists='append', index=False, con=self.dbh.engine)
        if follows == 1:
            result = pd.DataFrame({'source': [seed, new_seed], 'target': [new_seed, seed]})
            result.to_sql('result', if_exists='append', index=False, con=self.dbh.engine)

        update_query = """
                        UPDATE friends
                        SET burned = 1
                        WHERE source = {source} AND target = {target}
                       """.format(source=seed, target=new_seed)

        self.dbh.engine.execute(update_query)

        return new_seed

    def start_collectors(self, initial_number_of_collectors=2):
        pass
