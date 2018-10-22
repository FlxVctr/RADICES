from __future__ import unicode_literals
import tweepy as tp
from setup import FileImport
import webbrowser
import csv


class OAuthorizer():
    def __init__(self):
        ctoken, csecret = FileImport().read_app_key_file()
        auth = tp.OAuthHandler(ctoken, csecret)

        try:
            redirect_url = auth.get_authorization_url()
        except tp.TweepError as e:
            print("Error! Failes to get the request token.")
            raise e

        webbrowser.open(redirect_url)
        token = auth.request_token["oauth_token"]
        verifier = input("Please enter Verifier Code: ")
        auth.request_token = {'oauth_token': token,
                              'oauth_token_secret': verifier}
        try:
            auth.get_access_token(verifier)
        except tp.TweepError as e:
            print("Failed to get access token!")
            raise e

        with open('tokens.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow([auth.access_token, auth.access_token_secret])
        f.close()


testi = OAuthorizer()
