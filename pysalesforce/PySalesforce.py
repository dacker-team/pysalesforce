import copy
import datetime
import json
import os
import random

import requests

from pysalesforce.auth import get_access_token


class DBStream:

    def __init__(self, instance_name, client_id):
        self.instance_name = instance_name
        self.instance_type_prefix = ""
        self.ssh_init_port = ""
        self.client_id = client_id
        self.ssh_tunnel = None
        self.dbstream_instance_id = 'df-' + datetime.datetime.now().strftime('%s') + '-' + str(
            random.randint(1000, 9999))


class pysalesforce:

    def __init__(self):
        self.client="client"
        self.objects=[]
        self.access_token="null"

    def auth(self):
        self.access_token=get_access_token(self.client)

    def get_objects(self):
        params = {"Authorization":"Bearer "+self.access_token}
        url = "https://%s.my.salesforce.com/services/data/v44.0/sobjects/" %self.client
        result=requests.get(url, params=params).json()
        self.objects=[r["name"] for r in result.get("sobjects")]