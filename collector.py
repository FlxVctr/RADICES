import multiprocessing.dummy as mp
import time
from exceptions import TestException
from functools import wraps
from sys import stdout, stderr

import numpy as np
import pandas as pd
import tweepy
from sqlalchemy.exc import IntegrityError, ProgrammingError

from database_handler import DataBaseHandler
from helpers import friends_details_dtypes
from setup import FileImport

# mp.set_start_method('spawn')


def get_latest_tweets(user_id, connection, fields=['lang', 'full_text']):

    statuses = connection.api.user_timeline(user_id=user_id, count=200, tweet_mode='extended')

    result = pd.DataFrame(columns=fields)

    for status in statuses:
        result = result.append({field: getattr(status, field) for field in fields},
                               ignore_index=True)

    return result


def get_fraction_of_tweets_in_language(tweets):
    """Returns fraction of languages in a tweet dataframe as a dictionary

    Args:
        tweets (pandas.DataFrame): Tweet DataFrame as returned by `get_latest_tweets`
    Returns:
        language_fractions (dict): {languagecode (str): fraction (float)}
    """

    language_fractions = tweets['lang'].value_counts(normalize=True)

    language_fractions = language_fractions.to_dict()

    return language_fractions


# TODO: there might be a better way to drop columns that we don't want than flatten everything
# and removing the columns thereafter.
def flatten_json(y: dict, columns: list, sep: str = "_",
                 nonetype: dict = {'date': None, 'num': None, 'str': None, 'bool': None}):
    '''
    Flattens nested dictionaries.
    adapted from: https://medium.com/@amirziai/flattening-json-objects-in-python-f5343c794b10
    Attributes:
        y (dict): Nested dictionary to be flattened.
        columns (list of str): Dictionary keys that should not be flattened.
        sep (str): Separator for new dictionary keys of nested structures.
        nonetype (Value): specify the value that should be used if a key's value is None
    '''

    out = {}

    def flatten(x, name=''):
        if type(x) is dict and str(name[:-1]) not in columns:  # don't flatten nested fields
            for a in x:
                flatten(x[a], name + a + sep)
        elif type(x) is list and str(name[:-1]) not in columns:  # same
            i = 0
            for a in x:
                flatten(a, name + str(i) + sep)
                i += 1
        elif type(x) is list and str(name[:-1]) in columns:
            out[str(name[:-1])] = str(x)  # Must be str so that nested lists are written to db
        elif type(x) is dict and str(name[:-1]) in columns:
            out[str(name[:-1])] = str(x)  # Same here
        elif type(x) is bool and str(name[:-1]) in columns:
            out[str(name[:-1])] = int(x)  # Same here
        elif x is None and str(name[:-1]) in columns:
            if friends_details_dtypes[str(name[:-1])] == np.datetime64:
                out[str(name[:-1])] = nonetype["date"]
            elif friends_details_dtypes[str(name[:-1])] == np.int64:
                out[str(name[:-1])] = nonetype["num"]
            elif friends_details_dtypes[str(name[:-1])] == str:
                out[str(name[:-1])] = nonetype["str"]
            elif friends_details_dtypes[str(name[:-1])] == np.int8:
                out[str(name[:-1])] = nonetype["bool"]
            else:
                raise NotImplementedError("twitter user_detail does not have a supported"
                                          "corresponding data type")
        else:
            out[str(name[:-1])] = x

    flatten(y)
    return out


# Decorator function for re-executing x times (with exponentially developing
# waiting times)
def retry_x_times(x):
    def retry_decorator(func):

        @wraps(func)
        def func_wrapper(*args, **kwargs):

            try:
                if kwargs['fail'] is True:
                    # if we're testing fails:
                    return func(*args, **kwargs)
            except KeyError:
                try:
                    if kwargs['test_fail'] is True:
                        return func(*args, **kwargs)
                except KeyError:
                    pass

            i = 0
            if 'restart' in kwargs:
                restart = kwargs['restart']

            if 'retries' in kwargs:
                retries = kwargs['retries']
            else:
                retries = x

            for i in range(retries - 1):
                try:
                    if 'restart' in kwargs:
                        kwargs['restart'] = restart
                    return func(*args, **kwargs)
                except Exception as e:
                    restart = True
                    waiting_time = 2**i
                    stdout.write(f"Encountered exception in {func.__name__}{args, kwargs}.\n{e}")
                    stdout.write(f"Retrying in {waiting_time}.\n")
                    stdout.flush()
                    time.sleep(waiting_time)
                i += 1

            return func(*args, **kwargs)

        return func_wrapper

    return retry_decorator


class MyProcess(mp.Process):
    def run(self):
        try:
            mp.Process.run(self)
        except Exception as err:
            self.err = err
            raise self.err
        else:
            self.err = None


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

            for token, secret in self.tokens.values:
                self.token_queue.put((token, secret, {}, {}))
        else:
            self.token_queue = token_queue

        self.token, self.secret, self.reset_time_dict, self.calls_dict = self.token_queue.get()
        self.auth = tweepy.OAuthHandler(self.ctoken, self.csecret)
        self.auth.set_access_token(self.token, self.secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=False, wait_on_rate_limit_notify=False)

    def next_token(self):

        self.token_queue.put((self.token, self.secret, self.reset_time_dict, self.calls_dict))

        (self.token, self.secret,
         self.reset_time_dict, self.calls_dict) = self.token_queue.get()

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

    def __init__(self, connection, seed, following_pages_limit=0):
        self.seed = seed
        self.connection = connection

        self.token_blacklist = {}
        self.following_pages_limit = following_pages_limit

    class Decorators(object):

        @staticmethod
        def retry_with_next_token_on_rate_limit_error(func):
            def wrapper(*args, **kwargs):
                collector = args[0]
                old_token = collector.connection.token
                while True:
                    try:
                        try:
                            if kwargs['force_retry_token'] is True:
                                print('Forced retry with token.')
                                return func(*args, **kwargs)
                        except KeyError:
                            pass
                        try:
                            if collector.token_blacklist[old_token] <= time.time():
                                print(f'Token starting with {old_token[:4]} should work again.')
                                return func(*args, **kwargs)
                            else:
                                print(f'Token starting with {old_token[:4]} not ready yet.')
                                collector.connection.next_token()
                                time.sleep(10)
                                continue
                        except KeyError:
                            print(f'Token starting with {old_token[:4]} not tried yet. Trying.')
                            return func(*args, **kwargs)
                    except tweepy.RateLimitError:
                        collector.token_blacklist[old_token] = time.time() + 150
                        print(f'Token starting with {old_token[:4]} hit rate limit.')
                        print("Retrying with next available token.")
                        print(f"Blacklisted until {collector.token_blacklist[old_token]}")
                        collector.connection.next_token()
                        continue
                    break
            return wrapper

    @Decorators.retry_with_next_token_on_rate_limit_error
    def check_API_calls_and_update_if_necessary(self, endpoint, check_calls=True):
        """Checks for an endpoint how many calls are left (optional), gets the reset time
        and updates token if necessary.

        If called with check_calls = False,
        it will assume that the actual token calls for the specified endpoint are depleted
        and return None for remaining calls

        Args:
            endpoint (str): API endpoint, e.g. '/friends/ids'
            check_calls (boolean): Default True
        Returns:
            if check_calls=True:
                remaining_calls (int)
            else:
                None
        """

        def try_remaining_calls_except_invalid_token():
            try:
                remaining_calls = self.connection.remaining_calls(endpoint=endpoint)
            except tweepy.error.TweepError as invalid_error:
                if "'code': 89" in invalid_error.reason:
                    print(f"Token starting with {self.connection.token[:5]} seems to have expired or\
     it has been revoked.")
                    print(invalid_error)
                    self.connection.next_token()
                    remaining_calls = self.connection.remaining_calls(endpoint=endpoint)
                else:
                    raise invalid_error
            print("REMAINING CALLS FOR {} WITH TOKEN STARTING WITH {}: ".format(
                endpoint, self.connection.token[:4]), remaining_calls)
            return remaining_calls

        if check_calls is True:
            self.connection.calls_dict[endpoint] = try_remaining_calls_except_invalid_token()

            reset_time = self.connection.reset_time(endpoint=endpoint)

            self.connection.reset_time_dict[endpoint] = time.time() + reset_time

            while self.connection.calls_dict[endpoint] == 0:
                stdout.write("Attempt with next available token.\n")

                self.connection.next_token()

                try:
                    next_reset_at = self.connection.reset_time_dict[endpoint]
                    if time.time() >= next_reset_at:
                        self.connection.calls_dict[endpoint] = \
                            self.connection.remaining_calls(endpoint=endpoint)
                    else:
                        time.sleep(10)
                        continue
                except KeyError:
                    self.connection.calls_dict[endpoint] = \
                        try_remaining_calls_except_invalid_token()
                    reset_time = self.connection.reset_time(endpoint=endpoint)
                    self.connection.reset_time_dict[endpoint] = time.time() + reset_time

                print("REMAINING CALLS FOR {} WITH TOKEN STARTING WITH {}: ".format(
                    endpoint, self.connection.token[:4]), self.connection.calls_dict[endpoint])
                print(f"{time.strftime('%c')}: new reset of token {self.connection.token[:4]} for \
{endpoint} in {int(self.connection.reset_time_dict[endpoint] - time.time())} seconds.")

            return self.connection.calls_dict[endpoint]

        else:
            self.connection.calls_dict[endpoint] = 0

            if endpoint not in self.connection.reset_time_dict \
               or self.connection.reset_time_dict[endpoint] <= time.time():
                reset_time = self.connection.reset_time(endpoint=endpoint)
                self.connection.reset_time_dict[endpoint] = time.time() + reset_time
                print("REMAINING CALLS FOR {} WITH TOKEN STARTING WITH {}: ".format(
                    endpoint, self.connection.token[:4]), self.connection.calls_dict[endpoint])
                print(f"{time.strftime('%c')}: new reset of token {self.connection.token[:4]} for \
{endpoint} in {int(self.connection.reset_time_dict[endpoint] - time.time())} seconds.")

            while (endpoint in self.connection.reset_time_dict and
                   self.connection.reset_time_dict[endpoint] >= time.time() and
                   self.connection.calls_dict[endpoint] == 0):
                self.connection.next_token()
                time.sleep(1)

            return None

    def get_friend_list(self, twitter_id=None, follower=False):
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

        cursor = -1
        following_page = 0
        while self.following_pages_limit == 0 or following_page < self.following_pages_limit:
            while True:
                try:
                    if follower is False:
                        page = self.connection.api.friends_ids(user_id=twitter_id, cursor=cursor)
                        self.connection.calls_dict['/friends/ids'] = 1
                    else:
                        page = self.connection.api.followers_ids(user_id=twitter_id, cursor=cursor)
                        self.connection.calls_dict['/followers/ids'] = 1
                    break
                except tweepy.RateLimitError:
                    if follower is False:
                        self.check_API_calls_and_update_if_necessary(endpoint='/friends/ids',
                                                                     check_calls=False)
                    else:
                        self.check_API_calls_and_update_if_necessary(endpoint='/followers/ids',
                                                                     check_calls=False)

            if len(page[0]) > 0:
                result += page[0]
            else:
                break
            cursor = page[1][1]

            following_page += 1

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

            while True:
                try:
                    try:
                        user_details += self.connection.api.lookup_users(user_ids=friends[i:j],
                                                                         tweet_mode='extended')
                    except tweepy.error.TweepError as e:
                        if "No user matches for specified terms." in e.reason:
                            stdout.write(f"No user matches for {friends[i:j]}")
                            stdout.flush()
                        else:
                            raise e
                    self.connection.calls_dict['/users/lookup'] = 1
                    break
                except tweepy.RateLimitError:
                    self.check_API_calls_and_update_if_necessary(endpoint='/users/lookup',
                                                                 check_calls=False)

            i += 100

        return user_details

    @staticmethod
    def make_friend_df(friends_details, select=["id", "followers_count", "status_lang",
                                                "created_at", "statuses_count"],
                       provide_jsons: bool = False, replace_nonetype: bool = True,
                       nonetype: dict = {'date': '1970-01-01',
                                         'num': -1,
                                         'str': '-1',
                                         'bool': -1}):
        """Transforms list of user details to pandas.DataFrame

        Args:
            friends_details (list of Tweepy user objects)
            select (list of str): columns to keep in DataFrame
            provide_jsons (boolean): If true, will treat friends_details as list of jsons. This
                                     allows creating a user details dataframe without having to
                                     download the details first. Note that the jsons must have the
                                     same format as the _json attribute of a user node of the
                                     Twitter API.
            replace_nonetype (boolean): Whether or not to replace values in the user_details that
                                        are None. Setting this to False is experimental, since code
                                        to avoid errors resulting from it has not yet been
                                        implemented. By default, missing dates will be replaced by
                                        1970/01/01, missing numericals by -1, missing strs by
                                        '-1', and missing booleans by -1.
                                        Use the 'nonetype' param to change the default.
            nonetype (dict): Contains the defaults for nonetype replacement (see docs for
                             'replace_nonetype' param).
                             {'date': 'yyyy-mm-dd', 'num': int, 'str': 'str', 'bool': int}

        Returns:
            pandas.DataFrame with these columns or selected as by `select`:
                ['contributors_enabled',
                 'created_at',
                 'default_profile',
                 'default_profile_image',
                 'description',
                 'entities_description_urls',
                 'entities_url_urls',
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
                 'status_contributors',
                 'status_coordinates',
                 'status_coordinates_coordinates',
                 'status_coordinates_type',
                 'status_created_at',
                 'status_entities_hashtags',
                 'status_entities_media',
                 'status_entities_symbols',
                 'status_entities_urls',
                 'status_entities_user_mentions',
                 'status_extended_entities_media',
                 'status_favorite_count',
                 'status_favorited',
                 'status_geo',
                 'status_geo_coordinates',
                 'status_geo_type',
                 'status_id',
                 'status_id_str',
                 'status_in_reply_to_screen_name',
                 'status_in_reply_to_status_id',
                 'status_in_reply_to_status_id_str',
                 'status_in_reply_to_user_id',
                 'status_in_reply_to_user_id_str',
                 'status_is_quote_status',
                 'status_lang',
                 'status_place',
                 'status_place_bounding_box_coordinates',
                 'status_place_bounding_box_type',
                 'status_place_contained_within',
                 'status_place_country',
                 'status_place_country_code',
                 'status_place_full_name',
                 'status_place_id',
                 'status_place_name',
                 'status_place_place_type',
                 'status_place_url',
                 'status_possibly_sensitive',
                 'status_quoted_status_id',
                 'status_quoted_status_id_str',
                 'status_retweet_count',
                 'status_retweeted',
                 'status_retweeted_status_contributors',
                 'status_retweeted_status_coordinates',
                 'status_retweeted_status_created_at',
                 'status_retweeted_status_entities_hashtags',
                 'status_retweeted_status_entities_media',
                 'status_retweeted_status_entities_symbols',
                 'status_retweeted_status_entities_urls',
                 'status_retweeted_status_entities_user_mentions',
                 'status_retweeted_status_extended_entities_media',
                 'status_retweeted_status_favorite_count',
                 'status_retweeted_status_favorited',
                 'status_retweeted_status_geo',
                 'status_retweeted_status_id',
                 'status_retweeted_status_id_str',
                 'status_retweeted_status_in_reply_to_screen_name',
                 'status_retweeted_status_in_reply_to_status_id',
                 'status_retweeted_status_in_reply_to_status_id_str',
                 'status_retweeted_status_in_reply_to_user_id',
                 'status_retweeted_status_in_reply_to_user_id_str',
                 'status_retweeted_status_is_quote_status',
                 'status_retweeted_status_lang',
                 'status_retweeted_status_place',
                 'status_retweeted_status_possibly_sensitive',
                 'status_retweeted_status_quoted_status_id',
                 'status_retweeted_status_quoted_status_id_str',
                 'status_retweeted_status_retweet_count',
                 'status_retweeted_status_retweeted',
                 'status_retweeted_status_source',
                 'status_retweeted_status_full_text',
                 'status_retweeted_status_truncated',
                 'status_source',
                 'status_full_text',
                 'status_truncated',
                 'statuses_count',
                 'suspended',
                 'time_zone',
                 'translator_type',
                 'url',
                 'verified'
                 'utc_offset'],
        """

        if not provide_jsons:
            json_list_raw = [friend._json for friend in friends_details]
        else:
            json_list_raw = friends_details
        json_list = []
        dtypes = {key: value for (key, value) in friends_details_dtypes.items() if key in select}
        for j in json_list_raw:
            flat = flatten_json(j, sep="_", columns=select, nonetype=nonetype)
            # In case that there are keys in the user_details json that are not in select
            newflat = {key: value for (key, value) in flat.items() if key in select}
            json_list.append(newflat)

        df = pd.json_normalize(json_list)

        for var in select:
            if var not in df.columns:
                if dtypes[var] == np.datetime64:
                    df[var] = pd.to_datetime(nonetype["date"])
                elif dtypes[var] == np.int64:
                    df[var] = nonetype["num"]
                elif dtypes[var] == str:
                    df[var] = nonetype["str"]
                elif dtypes[var] == np.int8:
                    df[var] = nonetype["bool"]
                else:
                    df[var] = np.nan
            else:
                if dtypes[var] == np.datetime64:
                    df[var] = df[var].fillna(pd.to_datetime(nonetype["date"]))
                elif dtypes[var] == np.int64:
                    df[var] = df[var].fillna(nonetype["num"])
                elif dtypes[var] == str:
                    df[var] = df[var].fillna(nonetype["str"])
                elif dtypes[var] == np.int8:
                    df[var] = df[var].fillna(nonetype["bool"])
                df[var] = df[var].astype(dtypes[var])

        df.sort_index(axis=1, inplace=True)
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

        # TODO: check remaining API calls

        friendship = self.connection.api.show_friendship(
            source_id=source, target_id=target)

        following = friendship[0].following

        return following


class Coordinator(object):
    """Selects a queue of seeds and coordinates the collection with collectors
    and a queue of tokens.
    """

    def __init__(self, seeds=2, token_file_name="tokens.csv", seed_list=None,
                 following_pages_limit=0):

        # Get seeds from seeds.csv
        self.seed_pool = FileImport().read_seed_file()

        # Create seed_list if none is given by sampling from the seed_pool
        if seed_list is None:

            self.number_of_seeds = seeds
            try:
                self.seeds = self.seed_pool.sample(n=self.number_of_seeds)
            except ValueError:  # seed pool too small
                stderr.write("WARNING: Seed pool smaller than number of seeds.\n")
                self.seeds = self.seed_pool.sample(n=self.number_of_seeds, replace=True)

            self.seeds = self.seeds[0].values
        else:
            self.number_of_seeds = len(seed_list)
            self.seeds = seed_list

        self.seed_queue = mp.Queue()

        for seed in self.seeds:
            self.seed_queue.put(seed)

        # Get authorized user tokens for app from tokens.csv
        self.tokens = FileImport().read_token_file(token_file_name)

        # and put them in a queue
        self.token_queue = mp.Queue()

        for token, secret in self.tokens.values:
            self.token_queue.put((token, secret, {}, {}))

        # Initialize DataBaseHandler for DB communication
        self.dbh = DataBaseHandler()
        self.following_pages_limit = following_pages_limit

    def bootstrap_seed_pool(self, after_timestamp=0):
        """Adds all collected user details, i.e. friends with the desired properties
        (e.g. language) of previously found seeds to the seed pool.

        Args:
            after_timestamp (int): filter for friends added after this timestamp. Default: 0
        Returns:
            None
        """

        seed_pool_size = len(self.seed_pool)
        stdout.write("Bootstrapping seeds.\n")
        stdout.write(f"Old size: {seed_pool_size}. Adding after {after_timestamp} ")
        stdout.flush()

        query = f"SELECT id FROM user_details WHERE UNIX_TIMESTAMP(timestamp) >= {after_timestamp}"

        more_seeds = pd.read_sql(query, self.dbh.engine)
        more_seeds.columns = [0]  # rename from id to 0 for proper append
        self.seed_pool = self.seed_pool.merge(more_seeds, how='outer', on=[0])

        seed_pool_size = len(self.seed_pool)
        stdout.write(f"New size: {seed_pool_size}\n")
        stdout.flush()

    def lookup_accounts_friend_details(self, account_id, db_connection=None, select="*"):
        """Looks up and retrieves details from friends of `account_id` via database.

        Args:
            account_id (int)
            db_connection (database connection/engine object)
            select (str): comma separated list of required fields, defaults to all available ("*")
        Returns:
            None, if no friends found.
            Otherwise DataFrame with all details. Might be empty if language filter is on.
        """

        if db_connection is None:
            db_connection = self.dbh.engine

        query = f"SELECT target from friends WHERE source = {account_id} AND burned = 0"
        friends = pd.read_sql(query, db_connection)

        if len(friends) == 0:
            return None
        else:
            friends = friends['target'].values
            friends = tuple(friends)
            if len(friends) == 1:
                friends = str(friends).replace(',', '')

            query = f"SELECT {select} from user_details WHERE id IN {friends}"
            friend_detail = pd.read_sql(query, db_connection)

            return friend_detail

    def choose_random_new_seed(self, msg, connection):
        new_seed = self.seed_pool.sample(n=1)
        new_seed = new_seed[0].values[0]

        if msg is not None:
            stdout.write(msg + "\n")
            stdout.flush()

        self.token_queue.put(
            (connection.token, connection.secret,
             connection.reset_time_dict, connection.calls_dict))

        self.seed_queue.put(new_seed)

        return new_seed

    def write_user_details(self, user_details):
        """Writes pandas.DataFrame `user_details` to MySQL table 'user_details'
        """

        try:
            user_details.to_sql('user_details', if_exists='append',
                                index=False, con=self.dbh.engine)

        except IntegrityError:  # duplicate id (primary key)
            temp_tbl_name = self.dbh.make_temp_tbl()
            user_details.to_sql(temp_tbl_name, if_exists="append", index=False,
                                con=self.dbh.engine)
            query = "REPLACE INTO user_details SELECT * FROM {};".format(
                    temp_tbl_name)
            self.dbh.engine.execute(query)
            self.dbh.engine.execute("DROP TABLE " + temp_tbl_name + ";")

    @retry_x_times(10)
    def work_through_seed_get_next_seed(self, seed, select=[], status_lang=None,
                                        connection=None, fail=False, **kwargs):
        """Takes a seed and determines the next seed and saves all details collected to db.

        Args:
            seed (int)
            select (list of str): fields to save to database, defaults to all
            status_lang (str): Twitter language code for language of last status to filter for,
                defaults to None
            connection (collector.Connection object)
        Returns:
            seed (int)
        """

        # For testing raise of errors while multithreading
        if fail is True:
            raise TestException

        if 'fail_hidden' in kwargs and kwargs['fail_hidden'] is True:
            raise TestException

        language_check_condition = (
            status_lang is not None and
            'language_threshold' in kwargs and
            kwargs['language_threshold'] > 0
        )

        keyword_condition = ('keywords' in kwargs and
                             kwargs['keywords'] is not None and
                             len(kwargs['keywords']) > 0)

        if connection is None:
            connection = Connection(token_queue=self.token_queue)

        friends_details = None
        if 'restart' in kwargs and kwargs['restart'] is True:
            print("No db lookup after restart allowed, accessing Twitter API.")
        else:
            try:
                friends_details = self.lookup_accounts_friend_details(
                    seed, self.dbh.engine)

            except ProgrammingError:

                print("""Accessing db for friends_details failed. Maybe database does not exist yet.
                Accessing Twitter API.""")

        if friends_details is None:
            if 'restart' in kwargs and kwargs['restart'] is True:
                pass
            elif language_check_condition or keyword_condition:
                check_exists_query = f"""
                                        SELECT EXISTS(
                                            SELECT source FROM result
                                            WHERE source={seed}
                                            )
                                     """
                seed_depleted = self.dbh.engine.execute(check_exists_query).scalar()

                if seed_depleted == 1:
                    new_seed = self.choose_random_new_seed(
                        f'Seed {seed} is depleted. No friends meet conditions. Random new seed.',
                        connection)

                    return new_seed

            collector = Collector(connection, seed,
                                  following_pages_limit=self.following_pages_limit)

            try:
                friend_list = collector.get_friend_list()
                if 'bootstrap' in kwargs and kwargs['bootstrap'] is True:
                    follower_list = collector.get_friend_list(follower=True)
            except tweepy.error.TweepError as e:  # if account is protected
                if "Not authorized." in e.reason:

                    new_seed = self.choose_random_new_seed(
                        "Account {} protected, selecting random seed.".format(seed), connection)

                    return new_seed

                elif "does not exist" in e.reason:

                    new_seed = self.choose_random_new_seed(
                        f"Account {seed} does not exist. Selecting random seed.", connection)

                    return new_seed

                else:
                    raise e

            if friend_list == []:  # if account follows nobody

                new_seed = self.choose_random_new_seed(
                    "No friends or unburned connections left, selecting random seed.", connection)

                return new_seed

            self.dbh.write_friends(seed, friend_list)

            friends_details = collector.get_details(friend_list)
            select = list(set(select + ["id", "followers_count",
                                        "status_lang", "created_at", "statuses_count"]))
            friends_details = Collector.make_friend_df(friends_details, select)

            if 'bootstrap' in kwargs and kwargs['bootstrap'] is True:
                follower_details = collector.get_details(follower_list)
                follower_details = Collector.make_friend_df(follower_details, select)

            if status_lang is not None:

                if type(status_lang) is str:
                    status_lang = [status_lang]
                friends_details = friends_details[friends_details['status_lang'].isin(status_lang)]

                if 'bootstrap' in kwargs and kwargs['bootstrap'] is True:
                    follower_details = follower_details[follower_details['status_lang'].isin(
                        status_lang)]

                if len(friends_details) == 0:

                    new_seed = self.choose_random_new_seed(
                        f"No friends found with language '{status_lang}', selecting random seed.",
                        connection)

                    return new_seed

            self.write_user_details(friends_details)

            if 'bootstrap' in kwargs and kwargs['bootstrap'] is True:
                self.write_user_details(follower_details)

        if status_lang is not None and len(friends_details) == 0:

            new_seed = self.seed_pool.sample(n=1)
            new_seed = new_seed[0].values[0]

            stdout.write(
                "No user details for friends with last status language '{}' found in db.\n".format(
                    status_lang))
            stdout.flush()

            self.token_queue.put(
                (connection.token, connection.secret,
                 connection.reset_time_dict, connection.calls_dict))

            self.seed_queue.put(new_seed)

            return new_seed

        if 'restart' in kwargs and kwargs['restart'] is True:
            #  lookup just in case we had them already
            friends_details_db = self.lookup_accounts_friend_details(
                seed, self.dbh.engine)
            if friends_details_db is not None and len(friends_details_db) > 0:
                friends_details = friends_details_db

        double_burned = True

        while double_burned is True:
            max_follower_count = friends_details['followers_count'].max()

            new_seed = friends_details[
                friends_details['followers_count'] == max_follower_count]['id'].values[0]

            while language_check_condition or keyword_condition:
                # RETRIEVE AND TEST MORE TWEETS FOR LANGUAGE OR KEYWORDS
                try:
                    latest_tweets = get_latest_tweets(new_seed, connection,
                                                      fields=['lang', 'full_text'])
                except tweepy.error.TweepError as e:  # if account is protected
                    if "Not authorized." in e.reason:
                        new_seed = self.choose_random_new_seed(
                            f"Account {new_seed} protected, selecting random seed.", connection)

                        return new_seed
                    elif "does not exist" in e.reason:
                        new_seed = self.choose_random_new_seed(
                            f"Account {seed} does not exist. Selecting random seed.", connection)

                        return new_seed
                    else:
                        raise e

                threshold_met = True  # set true per default and change to False if not met
                keyword_met = True

                if language_check_condition:
                    language_fractions = get_fraction_of_tweets_in_language(latest_tweets)

                    threshold_met = any(kwargs['language_threshold'] <= fraction
                                        for fraction in language_fractions.values())

                if keyword_condition:
                    keyword_met = any(latest_tweets['full_text'].str.contains(keyword,
                                      case=False).any()
                                      for keyword in kwargs['keywords'])

                # THEN REMOVE FROM friends_details DATAFRAME, SEED POOL,
                # AND DATABASE IF FALSE POSITIVE
                # ACCORDING TO THRESHOLD OR KEYWORD

                if threshold_met and keyword_met:
                    break
                else:
                    friends_details = friends_details[friends_details['id'] != new_seed]

                    print(
                        f'seed pool size before removing not matching seed: {len(self.seed_pool)}')
                    self.seed_pool = self.seed_pool[self.seed_pool[0] != new_seed]
                    print(
                        f'seed pool size after removing not matching seed: {len(self.seed_pool)}')

                    # query = f"DELETE from user_details WHERE id = {new_seed}"
                    # self.dbh.engine.execute(query)

                    query = f"DELETE from friends WHERE target = {new_seed}"
                    self.dbh.engine.execute(query)

                    # AND REPEAT THE CHECK
                    try:
                        new_seed = friends_details[friends_details['followers_count'] ==
                                                   max_follower_count]['id'].values[0]
                    except IndexError:  # no more friends
                        new_seed = self.choose_random_new_seed(
                            f'{seed}: No friends meet set conditions. Selecting random.',
                            connection)

                        return new_seed

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

                try:
                    follows = int(collector.check_follows(source=new_seed, target=seed))
                except tweepy.TweepError:
                    print(f"Follow back undetermined. User {new_seed} not available")
                    follows = 0

            if follows == 0:

                insert_query = f"""
                    INSERT INTO result (source, target)
                    VALUES ({seed}, {new_seed})
                    ON DUPLICATE KEY UPDATE source = source
                """

                self.dbh.engine.execute(insert_query)

                print('\nno follow back: added ({seed})-->({new_seed})'.format(
                    seed=seed, new_seed=new_seed
                ))

            if follows == 1:

                insert_query = f"""
                    INSERT INTO result (source, target)
                    VALUES
                        ({seed}, {new_seed}),
                        ({new_seed}, {seed})
                    ON DUPLICATE KEY UPDATE source = source
                """

                self.dbh.engine.execute(insert_query)

                print('\nfollow back: added ({seed})<-->({new_seed})'.format(
                    seed=seed, new_seed=new_seed
                ))

            update_query = """
                            UPDATE friends
                            SET burned=1
                            WHERE source={source} AND target={target} AND burned = 0
                           """.format(source=seed, target=new_seed)

            update_result = self.dbh.engine.execute(update_query)

            if update_result.rowcount == 0:
                print(f"Connection ({seed})-->({new_seed}) was burned already.")
                friends_details = self.lookup_accounts_friend_details(
                    seed, self.dbh.engine)

                if friends_details is None or len(friends_details) == 0:
                    new_seed = self.choose_random_new_seed(
                        f"No friends or unburned connections left for {seed}, selecting random.",
                        connection)

                    return new_seed

            else:
                print(f"burned ({seed})-->({new_seed})")
                double_burned = False

        self.token_queue.put(
            (connection.token, connection.secret,
             connection.reset_time_dict, connection.calls_dict))

        self.seed_queue.put(new_seed)

        return new_seed

    def start_collectors(self, number_of_seeds=None, select=[], status_lang=None, fail=False,
                         fail_hidden=False, restart=False, retries=10, bootstrap=False,
                         latest_start_time=0, language_threshold=0, keywords=[]):
        """Starts `number_of_seeds` collector threads
        collecting the next seed for on seed taken from `self.queue`
        and puting it back into `self.seed_queue`.

        Args:
            number_of_seeds (int): Defaults to `self.number_of_seeds`
            select (list of strings): fields to save to user_details table in database
            status_lang (str): language code for latest tweet langage to select
        Returns:
            list of mp.(dummy.)Process
        """

        if bootstrap is True:

            if restart is True:
                latest_start_time = 0

            self.bootstrap_seed_pool(after_timestamp=latest_start_time)

        if number_of_seeds is None:
            number_of_seeds = self.number_of_seeds

        processes = []
        seed_list = []

        print("number of seeds: ", number_of_seeds)

        for i in range(number_of_seeds):
            seed = self.seed_queue.get()
            seed_list += [seed]
            print("seed ", i, ": ", seed)
            processes.append(MyProcess(target=self.work_through_seed_get_next_seed,
                                       kwargs={'seed': seed,
                                               'select': select,
                                               'status_lang': status_lang,
                                               'fail': fail,
                                               'fail_hidden': fail_hidden,
                                               'restart': restart,
                                               'retries': retries,
                                               'language_threshold': language_threshold,
                                               'bootstrap': bootstrap,
                                               'keywords': keywords},
                                       name=str(seed)))

        latest_seeds = pd.DataFrame(seed_list)

        latest_seeds.to_csv('latest_seeds.csv', index=False, header=False)

        for p in processes:
            p.start()
            print(f"Thread {p.name} started.")

        return processes
