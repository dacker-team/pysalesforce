import os
import requests


def get_token_and_base_url(client, salesforce_test_instance=False):
    params = {
        "grant_type": "password",
        "client_id": os.environ["SALESFORCE_%s_CLIENT_ID" % client],
        "client_secret": os.environ["SALESFORCE_%s_CLIENT_SECRET" % client],
        "username": os.environ["SALESFORCE_%s_USERNAME" % client],
        "password": os.environ["SALESFORCE_%s_PASSWORD" % client] + os.environ["SALESFORCE_%s_SECURITY_TOKEN" % client],
    }
    if not salesforce_test_instance:
        url = "https://login.salesforce.com/services/oauth2/token"
    else:
        url = "https://test.salesforce.com/services/oauth2/token"

    r = requests.post(url, params=params).json()
    if r.get("error"):
        return r
    else:
        return r.get("access_token"), r.get('instance_url')
