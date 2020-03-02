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

    def get_all_objects(self):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = "https://%s.my.salesforce.com/services/data/v44.0/sobjects" % self.client
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["sobjects"]]

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        objects = []
        for o in _objects:
            if o["api_name"] in self.get_all_objects():
                objects.append(o["api_name"])
            else:
                print (o["api_name"] + " does not exist.")
        return objects

    def get_table_names(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        tables = []
        for o in _objects:
            if o["api_name"] in self.get_all_objects():
                tables.append(o["name"])
            else:
                print (o["name"] + " does not exist.")
        return tables

    def get_all_fields(self,object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = "https://%s.my.salesforce.com/services/data/v44.0/sobjects/%s/describe" % (self.client,object_name)
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["fields"]]

    # def get_fields(self,object_name):
        # fields = []
        # if object_name in self.get_objects():
        #     config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        #     _fields = config.get("objects")['fields']
        #     print(_fields)
        #     for f in _fields:
        #         if f[0] in self.get_all_fields(object_name):
        #             fields.append(f[0])
        #         else:
        #             print(f[0] + " does not exist.")
        # return fields

    def query (self,object_name):
        fields=self.get_all_fields(object_name)
        query='select '
        for p in fields:
            query+=p+','
        query=query[:-1]
        query+=' from '+object_name
        return query

    def execute_query(self,query):
        result = []
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        params = {
            "q": query
        }
        BASE_URL = "https://%s.my.salesforce.com" % self.client
        url = BASE_URL + "/services/data/v44.0/query/"
        r = requests.get(url, params=params, headers=headers).json()
        result = result + r["records"]
        next_records_url = r.get('nextRecordsUrl')
        while next_records_url:
            r = requests.get(BASE_URL + next_records_url, headers=headers).json()
            result = result + r["records"]
            next_records_url = r.get('nextRecordsUrl')
        return {"records": result}
