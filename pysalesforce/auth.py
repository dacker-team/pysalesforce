import os
import requests

def get_access_token(client):
    params = {
        "grant_type": "password",
        "client_id": os.environ["SALESFORCE_%s_CLIENT_ID" %client],
        "client_secret": os.environ["SALESFORCE_CLIENT_SECRET"],
        "username": os.environ["SALESFORCE_USERNAME"],
        "password": os.environ["SALESFORCE_PASSWORD"] + os.environ["SALESFORCE_SECURITY_TOKEN"],
    }
    url = "https://login.salesforce.com/services/oauth2/token"
    return requests.post(url, params=params).json()