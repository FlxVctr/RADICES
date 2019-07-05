# SparseTwitter

Comprehensive Description of Project / Project Goal / Functioning of Code
Project to create a sparsified sample network of (German)Twitter Users

## Disclaimer
By submitting a pull request to this repository, you agree to license your contribution under the MIT license (as this project is).

## How it works
1. Create a Twitter Developer app (read more here) #TODO
2. Have users authorise your app (the more the better - at least one) (see Step X) TODO
3. Set up a mysql Database locally or online. (See here for reference) (TODO)
4. Fill out config.yml according to your requirements (See here) TODO
5. Fill out the seeds_template with your starting seeds or use the given ones (See here) TODO
6. Set up your virtual environment with pipenv (LINK TODO)
7. Start software, be happy
8. (Develop the app further - see tests)

### Authorise App & Get Tokens
This app is based on a [Twitter Developer](https://developer.twitter.com/) app. To use it you have to first create a Twitter app.
Once you did that, your Consumer API Key and Secret have to be pasted into `empty_keys.json`. Then, rename it to `keys.json`.
You are now ready to have users authorize your app so that it will get more API calls. To do so, run
```
python twauth.py
```
This will open a link to Twitter that requires you (or someone else) to log in with their Twitter account. Once logged in, a 6-digit authorisation key will be shown on the screen. This key has to be entered into the console window where `twauth.py` is still running. After the code was entered, a new token will be added to the `tokens.csv` file (which is created, if it does not exist). For this software to run, the app has to be authorised by at least one Twitter user.

### Configuration (config.yml)
After setting up your mysql database, open `config_template.yml` and enter the database information. Do not change the dbtype argument since at the moment, only mySQL databases are supported.
Note that the password field is required (this also means that your database has to be password-protected). If no password is given (even is none is needed for the database), the app will raise an Exception.

You can also indicate which Twitter user account details you want to collect. Those will be stored in a database table called `user_details`. By default, the software collects account id, follower count, account creation time and account tweets count. If you wish to collect more user details, just enter the mysql type after the colon (":") of the respective user detail in the list. The suggested type is already indicated in the comment in the respective line. Note, however, that collecting huge amounts of data has not been tested with all the user details being collected, so we do not guarantee the code to work with them. Moreover, due to Twitter API changes, some of the user details may become private properties, thus not collectable any more through the API.

If you have a mailgun account, you can also add your details at the bottom of the `config.yml`. If you do so, you will receive an email when the software encounters an error.

### Indicate starting seeds for the walkers
The algorithm needs seeds (i.e. Twitter Account IDs) to draw randomly from when initialising the walkers or when it reached an impasse. These seeds have to be specified in `seeds.csv`. One Twitter ID per line. Feel free to use `seeds_template.csv` (and rename it to `seeds.csv`) or replace the existing seeds which are ~200 randomly drawn accounts from the TrISMA dataset (Bruns, Moon, Münch & Sadkowsky, 2017) that use German as interface language.

Note that the `seeds.csv` at least have to contain that many account IDs as walkers should run in parallel. We suggest using at least 100 seeds, the more the better (we used 15.000.000). However, in a later update, the algorithm will subsequently gather its own seeds and there will be no need to give a comprehensive seed list

### Pipenv (TODO CONTINUE HERE)
We highly recommend installing [pipenv](https://pipenv.readthedocs.io/en/latest/) to create a virtual environment with all the required packages in the respective versions.
After installing pipenv, navigate to the project directory and run:

```
pipenv install
```
This creates a virtual environment and installs the packages specified in the Pipfile.

Run
```
pipenv shell
```
to start a shell in the virtual environment.

## Start
Run
```
python start.py -n 2 -p 1
```
where n defines the number of seeds to be drawn from the seed pool and p the number of pages to look at when identifying the next node.

Note:

- If the program freezes after saying "Starting x Collectors", it is likely that either your keys.json or your tokens.csv contains wrong information. We work on a solution that is more user-friendly!
- If you get an error saying "lookup_users() got an unexpected keyword argument", you likely have the wrong version of tweepy installed. Either update your tweepy package or use pipenv to create a virtual environment and install all the packages you need.


## Testing

For development purposes. Note that you still need a functional (i.e. filled out) `keys.json` and tokens indicated in `tokens.csv` to work with.
Moreover, for some tests to run through, test tweet json files are needed. They have to be stored in `tests/tweet_jsons/` and can be downloaded
TODO

### passwords.py
Before testing, please re-enter the password of the sparsetwitter mySQL user into the `passwords_template.py`. Then, rename it into `passwords.py`. If you would like to make use (and test) mailgun notifications, please also enter the relevant information as well.

### Local mysql database
Some of the tests try to connect to a local mySQL database using the user "sparsetwitter@localhost". For these tests to run properly it is required that a mySQL server actually runs on the device and that a user 'sparsetwitter'@'localhost' with relevant permissions exists.

Please refer to the [mySQL documentation](https://dev.mysql.com/doc/mysql-installation-excerpt/5.5/en/installing.html) on how to install mySQL on your system. If your mySQL server is up and running, the following command will create the user 'sparsetwitter' and will give it full permissions (replace "<your password>" with a password):

```
CREATE USER 'sparsetwitter'@'localhost' IDENTIFIED BY '<your password>'; GRANT ALL ON *.* TO 'sparsetwitter'@'localhost' WITH GRANT OPTION;
```

### Tests that will fail
For the functional tests, the test `FirstUseTest.test_restarts_after_exception` will fail if you did not provide (or did not provide valid) Mailgun credentials.

### Running the tests
To run the tests, just type

```
python functional_test.py
```

and / or
```
python tests/tests.py
```

# TODO
- Testing (add paragraph about make_test_tweet_jsons)
