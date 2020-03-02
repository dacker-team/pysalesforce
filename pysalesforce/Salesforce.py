import copy
import datetime
import json
import os
import random
import yaml
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


class Salesforce:

    def __init__(self, client):
        self.client = client
        self.config_file_path = 'pysalesforce/config.yaml'
        self.access_token = get_access_token(client)
        self.schema_prefix=True


    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        objects = []
        for o in _objects.keys():
            objects.append(o["api_name"])
        return objects

    def get_table_names(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        objects = []
        for o in _objects.keys():
            objects.append(o["name"])
        return objects
