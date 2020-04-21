import os
import requests

url = "https://login.salesforce.com/services/oauth2/token"


def get_access_token(client, url="https://login.salesforce.com/services/oauth2/token"):
    params = {
        "grant_type": "password",
        "client_id": os.environ["SALESFORCE_%s_CLIENT_ID" % client],
        "client_secret": os.environ["SALESFORCE_%s_CLIENT_SECRET" % client],
        "username": os.environ["SALESFORCE_%s_USERNAME" % client],
        "password": os.environ["SALESFORCE_%s_PASSWORD" % client] + os.environ["SALESFORCE_%s_SECURITY_TOKEN" % client],
    }
    r = requests.post(url, params=params).json()
    if r.get("error"):
        return r
    else:
        return r.get("access_token")
