import os
import requests

def get_access_token(client):
    params = {
        "grant_type": "password",
        "client_id": os.environ["SALESFORCE_%s_CLIENT_ID" %client],
        "client_secret": os.environ["SALESFORCE_%s_CLIENT_SECRET" %client],
        "username": os.environ["SALESFORCE_%s_USERNAME" %client],
        "password": os.environ["SALESFORCE_%s_PASSWORD"%client] + os.environ["SALESFORCE_%s_SECURITY_TOKEN"%client],
    }
    url = "https://login.salesforce.com/services/oauth2/token"
    r=requests.post(url, params=params).json()
    return r.get("access_token")