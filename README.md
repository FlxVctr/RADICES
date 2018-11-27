# SparseTwitter
Project to create a sparsified sample network of (German)Twitter Users


## Setup dev environment

Install [pipenv](https://pipenv.readthedocs.io/en/latest/) and run:

```
pipenv install
```
This installs the packages specified in the Pipfile into your virtual environment.

Run
```
pipenv shell
```
to start a shell in the virtual env.

## Testing

Before the testing can begin, you will need several files (filled):

-   passwords.py: stores the password for your mySQL database. Just insert your password into the passwords_template.py and rename it to passwords.py.

-   keys.json: Stores your app key and secret ("Consumer key, Consumer Secret", [get your own Twitter app](https://developer.twitter.com/)). Use empty_keys.json as a template.

-   tokens.csv: Stores the user access tokens who have authorized your app. The more you have, the more requests you'll be able to query to the Twitter API. You can create a tokens csv by executing twauth.py (only works if keys.json exists and contains app key and secret).

-   a number of tests/tweet_jsons/user_x.json files for testing. You can create the directory and files by running make_test_tweet_jsons.py.

<span style="color:red">/TODO: MORE DETAILED EXPLANATION OF THE SINGLE FILES</span>

After you created all these files, you should be ready to go for the testing. The unit and functional tests modules are located in `/tests`. Note that you will have to start the tests from the project root (i.e., using `python tests/tests.py` since it appends the PYTHONPATH by the current working directory.
