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

    def describe_objects(self,object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = "https://%s.my.salesforce.com/services/data/v44.0/sobjects/%s/describe" % (self.client,object_name)
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["fields"]]


    def query (self,object_name):
        fields=self.describe_objects(object_name)
        query='select '
        for p in fields:
            query+=p+','
        query=query[:-1]
        query+=' from '+object_name
        return query

    def execute_query(self,object_name,batch_size):
        result = []
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        params = {
            "q": self.query(object_name)
        }
        BASE_URL = "https://%s.my.salesforce.com" % self.client
        url = BASE_URL + "/services/data/v44.0/query/"
        r = requests.get(url, params=params, headers=headers).json()
        result = result + r["records"]
        next_records_url = r.get('nextRecordsUrl')
        totalsize=r.get('totalSize')
        print(r.get('totalSize'))
        i=1
        print(i)
        _continue = False
        while i < batch_size:
            _continue = True
            if not next_records_url:
                _continue = False
                break
            else:
                _continue = True
                while _continue:
                    r = requests.get(BASE_URL + next_records_url, headers=headers).json()
                    result = result + r["records"]
                    next_records_url = r.get('nextRecordsUrl')
                    totalsize = totalsize + r.get('totalSize')
                    print(r.get('nextRecordsUrl'))
                    print(r.get('totalSize'))
                    i = i + 1
                    print(i)
                    _continue = False
        return {"records": result,"object":object_name}

    def process_data(self, raw_data):
        object_row=[]
        for r in raw_data.get("records"):
            _object=dict()
            for o in self.describe_objects(raw_data.get("object")):
                _object[o.lower()]=r.get(o)
                object_row.append(_object)
        return object_row


    def get_column_names(self, data):
        column_list=[]
        for d in data:
            for c in d.keys():
                if c not in column_list:
                    column_list.append(c)
        return column_list
