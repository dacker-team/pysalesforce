import os
import requests

def get_access_token(client):
    params = {
        "grant_type": "password",
        "client_id": os.environ["SALESFORCE_%s_CLIENT_ID" %client],
        "client_secret": os.environ["SALESFORCE_%s_CLIENT_SECRET"],
        "username": os.environ["SALESFORCE_%s_USERNAME"],
        "password": os.environ["SALESFORCE_PASSWORD"] + os.environ["SALESFORCE_%s_SECURITY_TOKEN"],
    }
    url = "https://login.salesforce.com/services/oauth2/token"
    r=requests.post(url, params=params).json()
    return r.get("access_token")