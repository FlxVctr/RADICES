![LOGO](https://upload.wikimedia.org/wikipedia/commons/thumb/4/48/Radishes.svg/173px-Radishes.svg.png)

# RADICES

This software prototype creates an explorative sample of core accounts in (optionally language-based) Twitter follow networks.

It was developed first for a Twitter follow network sampling experiment described in this talk: https://youtu.be/qsnGTl8d3qU?t=21823. A journal article describing the method and its results in detail is currently undergoing peer review. Until then you can [cite the software itself](https://doi.org/10.6084/m9.figshare.8864777). A preprint is available here: https://arxiv.org/abs/1908.07788

(**PLEASE NOTE:** The language specification is not working as it did for our paper due to changes in the Twitter API. Now it uses the language of the last tweet (or optionally the last 200 tweets with a threshold fraction defined by you to avoid false positives) by a user as determined by Twitter instead of the interface language. This might lead to different results.)

Please feel free to open an issue or comment if you have any questions.

Moreover, if you find any bugs, you are invited to report them as an [Issue](https://github.com/FlxVctr/SparseTwitter/issues).

Before contributing/raising an issue, please read the [Contributor Guidelines](CONTRIBUTING.md).

## Installation & Usage
1. [Create a Twitter Developer app](https://developer.twitter.com/en/docs/basics/getting-started)
2. Set up your virtual environment with [pipenv](https://pipenv.readthedocs.io/en/latest/) [(see here)](#Create-Virtual-Environment-with-Pipenv)
3. Have users authorise your app (the more the better - at least one) [(see here)](#authorise-app--get-tokens)
4. [Set up a mysql Database locally or online](https://dev.mysql.com/doc/mysql-getting-started/en/).
5. Fill out config.yml according to your requirements [(see here)](#configuration-configyml)
6. Fill out the seeds_template with your starting seeds or use the given ones [(see here)](#Indicate-starting-seeds-for-the-walkers)
7. [Start software](#Start), be happy
8. (Develop the app further - [run tests](#Testing))

### Create Virtual Environment with Pipenv
We recommend installing [pipenv](https://pipenv.readthedocs.io/en/latest) (including the installation of pyenv) to create a virtual environment with all the required packages in the respective versions.
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

### Authorise App & Get Tokens
This app is based on a [Twitter Developer](https://developer.twitter.com/) app. To use it you have to first create a Twitter app.
Once you did that, your Consumer API Key and Secret have to be pasted into a `keys.json`, for which you can copy `empty_keys.json` (do not delete or change this file if you want to use the developer tests).
You are now ready to have users authorize your app so that it will get more API calls. To do so, run
```
python twauth.py
```
This will open a link to Twitter that requires you (or someone else) to log in with their Twitter account. Once logged in, a 6-digit authorisation key will be shown on the screen. This key has to be entered into the console window where `twauth.py` is still running. After the code was entered, a new token will be added to the `tokens.csv` file. For this software to run, the app has to be authorised by at least one Twitter user.

### Configuration (config.yml)
After setting up your mysql database, copy `config_template.yml` to a file named `config.yml` and enter the database information. Do not change the dbtype argument since at the moment, only mySQL databases are supported.
Note that the password field is required (this also means that your database has to be password-protected). If no password is given (even is none is needed for the database), the app will raise an Exception.

You can also indicate which Twitter user account details you want to collect. Those will be stored in a database table called `user_details`. By default, the software has to collect account id, follower count, account creation time and account tweets count at the moment and you have to activate those by uncommenting in the config. If you wish to collect more user details, just enter the mysql type after the colon (":") of the respective user detail in the list. The suggested type is already indicated in the comment in the respective line. Note, however, that collecting huge amounts of data has not been tested with all the user details being collected, so we do not guarantee the code to work with them. Moreover, due to Twitter API changes, some of the user details may become private properties, thus not collectable any more through the API.

If you have a mailgun account, you can also add your details at the bottom of the `config.yml`. If you do so, you will receive an email when the software encounters an error.

### Indicate starting seeds for the walkers
The algorithm needs seeds (i.e. Twitter Account IDs) to draw randomly from when initialising the walkers or when it reached an impasse. These seeds have to be specified in `seeds.csv`. One Twitter account ID per line. Feel free to use `seeds_template.csv` (and rename it to `seeds.csv`) to replace the existing seeds which are 200 randomly drawn accounts from the TrISMA dataset (Bruns, Moon, Münch & Sadkowsky, 2017) that use German as interface language.

Note that the `seeds.csv` at least have to contain that many account IDs as walkers should run in parallel. We suggest using at least 100 seeds, the more the better (we used 15.000.000). However, since a recent update, the algorithm can gather ('bootstrap') its own seeds and there is no need to give a comprehensive seed list. This changes the quality of the sample (for the worse or the better is subject of ongoing research), however, it makes it a very powerful exploratory tool.

## Start

**PLEASE NOTE:** The language specification is not working as it did for our paper due to changes in the Twitter API. Now it uses the language of the last tweet(s) by a user as determined by Twitter instead of the interface language. This might lead to different results from our paper (even though the macrostructures of a certain network should remain very similar).

Run (while you are in the pipenv virtual environment)
```
python start.py -n 2 -l de it -lt 0.05 -p 1 -k "keyword1" "keyword2" "keyword3"
```
where

* -n takes the number of seeds to be drawn from the seed pool,
* -l can set the Twitter accounts's last [status languages](https://developer.twitter.com/en/docs/developer-utilities/supported-languages/api-reference/get-help-languages) that are of your interest,
* -lt defines a fraction of tweets within the last 200 tweets that has to be detected to be in the requested languages (might slow down collection)
* -k can be used to only follow paths to seeds who used defined keywords in their last 200 tweets (keywords are interpreted as [regexes](https://docs.python.org/3/howto/regex.html), ignoring case)
* and -p the number of pages to look at when identifying the next node. For explanation of advanced usage and more features (like 'bootstrapping', an approach, reminiscent of snowballing, to grow the seed pool) use

```
python start.py --help
```
which will show a help dialogue with explanations and default values. Please raise an issue if those should not be clear enough.

Note:
- If the program freezes after saying "Starting x Collectors", it is likely that either your keys.json or your tokens.csv contains wrong information. We work on a solution that is more user-friendly!
- If you get an error saying "lookup_users() got an unexpected keyword argument", you likely have the wrong version of tweepy installed. Either update your tweepy package or use pipenv to create a virtual environment and install all the packages you need.
- If at some point an error is encountered: There is a -r (restart with latest seeds) option to resume collection after interrupting the crawler with `control-c`. This is also handy in case you need to reboot your machine. **Note that you will still have to define the other parameters as you did when you started the collection the first time.**

## Analysis (e.g. with Gephi)

It is possible to import the data into Gephi via the MySQL connector. However, Gephi apparently supports only MySQL 5 at the time of writing.

To do so, it is helpful to use `create_node_view.sql` and `create_dense_result.sql` to create views for Gephi to import.

Then you can import the results into Gephi via the menu item **File -> Import Database -> Edge List**, using your database credentials and

* `SELECT * FROM nodes` as the "Node Query"
* `SELECT * FROM result` as the "Edge Query" if you want to analyse the walked edges only (as done in the German Twittersphere paper)
* `SELECT * FROM dense_result` as the "Edge Query" if you want to analyse all edges between collected accounts (which will be a much denser network)

## Testing

For development purposes. Note that you still need a functional (i.e. filled out) `keys.json` and tokens indicated in `tokens.csv` to work with.
Moreover, for some tests to run through, some user details json files are needed. They have to be stored in `tests/tweet_jsons/` and can be downloaded by running
```
python make_test_tweet_jsons.py -s 1670174994
```
where -s stands for the seed to use and can be replaced by any Twitter seed of your choice.
Note: the name `tweet_jsons` is misleading, since the json files actually contain information about specific users (friends of the given seed). This will be changed in a later version.

### passwords.py
Before testing, please re-enter the password of the sparsetwitter mySQL user into the `passwords_template.py`. Then, rename it into `passwords.py`. If you would like to make use (and test) mailgun notifications, please also enter the relevant information as well.

### Local mysql database
Some of the tests try to connect to a local mySQL database using the user "sparsetwitter@localhost". For these tests to run properly it is required that a mySQL server actually runs on the device and that a user 'sparsetwitter'@'localhost' with relevant permissions exists.

Please refer to the [mySQL documentation](https://dev.mysql.com/doc/mysql-installation-excerpt/5.5/en/installing.html) on how to install mySQL on your system. If your mySQL server is up and running, the following command will create the user 'sparsetwitter' and will give it full permissions (replace "<your password>" with a password):

```
CREATE USER 'sparsetwitter'@'localhost' IDENTIFIED BY '<your password>'; GRANT ALL ON *.* TO 'sparsetwitter'@'localhost' WITH GRANT OPTION;
```

### Tests that will fail
For the functional tests, the test `FirstUseTest.test_restarts_after_exception` will fail if you did not provide (or did not provide valid) Mailgun credentials. Also one unit-test will fail in this case.

### Running the tests
To run the tests, just type

```
python functional_test.py
```

and / or

```
python tests/tests.py -s
```
The -s parameter is for skipping API call-draining tests. Note that even if -s is set, the tests can take very long to run if only few API tokens are given in the tokens.csv. The whole software relies on a sufficiently high number of tokens. We used 15.

## Disclaimer
By submitting a pull request to this repository, you agree to license your contribution under the MIT license (as this project is).

The "Logo" above is from https://commons.wikimedia.org/wiki/File:Radishes.svg and licensed as being in the public domain ([CC0](https://creativecommons.org/publicdomain/zero/1.0/deed.en)).

