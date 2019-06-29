# SparseTwitter

Comprehensive Description of Project / Project Goal / Functioning of Code
Project to create a sparsified sample network of (German)Twitter Users


## How it works
 -Get Twitter App
 -Have users authorise your app (recommended: at least 3)
 -Fill out config.yml
 -Indicate seeds
 -Set up environment with pipenv
 -Start Programme
 -(then testing)

## Prepare
Before using the app, some basic preparations have to be made.

### Config.yml
Open `config_template.yml` and enter the information about your mysql database. Do not change the dbtype argument since at the moment, only mySQL databases are supported.
Note: The password field is required! If no password is given (even is none is needed for the database), the app will raise an Exception. This means that the database user specified in config.yml has always to log in with a password, even when the mySQL server runs on localhost.

### Seeds
The algorithm needs seeds (i.e. Twitter Account IDs) to draw randomly from when it reached an impasse. These seeds have to be specified in `seeds.csv`. One Twitter ID per line. Feel free to use `seeds_template.csv` and replace the existing seeds which represent the Twitter accounts of Thomas Mueller (German soccer player) and Sascha Lobo (German blogger). For the algorithm to create a good sample, many seeds should be given (we used 15.000.000). However, in a later update, the algorithm will subsequently gather its own seeds.

### Tokens
This app is based on a [Twitter Developer](https://developer.twitter.com/) app. To use it you have to first create a Twitter app.
Once you did that, your Consumer API Key and Secret have to be pasted into `empty_keys.json`. Then, rename it to `keys.json`.
You are now ready to have users authorize your app so that it will get more API calls. To do so, run
```
python twauth.py
```
This will open a link to Twitter that requires you (or someone else) to log in with their Twitter account. Once logged in, a 6-digit authorisation key will be shown on the screen. This key has to be entered into the console window where `twauth.py` is still running. When they code was entered, it will be added to the `tokens.csv` file (which is created, if it does not exist). For the software to run, the app has to be authorised by at least one Twitter user.

### Pipenv
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

```diff
- If the program freezes after saying "Starting x Collectors", it is likely that either your keys.json or your tokens.csv contains wrong information. We work on a solution that is more user-friendly!
- If you get an error saying "lookup_users() got an unexpected keyword argument", you likely have the wrong version of tweepy installed. Either update your tweepy package or use pipenv to create a virtual environment and install all the packages you need.
```

## Testing

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




# Old
Before the testing can begin, you will need several files (filled):

-   passwords.py: stores the password for your mySQL database. Just insert your password into the passwords_template.py and rename it to passwords.py.

-   keys.json: Stores your app key and secret ("Consumer key, Consumer Secret", [get your own Twitter app](https://developer.twitter.com/)). Use empty_keys.json as a template.

-   tokens.csv: Stores the user access tokens who have authorized your app. The more you have, the more requests you'll be able to query to the Twitter API. You can create a tokens csv by executing twauth.py (only works if keys.json exists and contains app key and secret).

-   a number of tests/tweet_jsons/user_x.json files for testing. You can create the directory and files by running make_test_tweet_jsons.py.

<span style="color:red">/TODO: MORE DETAILED EXPLANATION OF THE SINGLE FILES</span>

After you created all these files, you should be ready to go for the testing. The unit and functional tests modules are located in `/tests`. Note that you will have to start the tests from the project root (i.e., using `python tests/tests.py` since it appends the PYTHONPATH by the current working directory.
