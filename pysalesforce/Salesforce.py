import copy
import datetime
import json
import os
import random
import time

import yaml
import requests
from pyred import RedDBStream

from pysalesforce.auth import get_access_token
from pysalesforce.date import start_end_from_last_call


class Salesforce:

    def __init__(self, client, clientid):
        self.client = client
        self.clientid = clientid
        self.config_file_path = 'pysalesforce/config.yaml'
        self.access_token = get_access_token(client)
        self.schema_prefix = 'salesforce_test.'

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        objects = []
        for o in _objects:
            o.append(o["api_name"])
        return objects

    def get_table_names(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        _objects = config.get("objects")
        tables = []
        for o in _objects:
                tables.append(o["name"])
        return tables

    def describe_objects(self, object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = "https://%s.my.salesforce.com/services/data/v44.0/sobjects/%s/describe" % (self.client, object_name)
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["fields"]]

    def query(self, object_name, since):
        fields = self.describe_objects(object_name)
        where_clause = ""
        if since:
            where_clause = " where lastmodifieddate >= %s" % since
        query = 'select '
        for p in fields:
            query += p + ','
        query = query[:-1]
        query += ' from ' + object_name + where_clause
        print(query)
        return query

    def execute_query(self, object_name, batch_size, since, next_records_url=None):
        result = []
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        params = {
            "q": self.query(object_name, since)
        }
        BASE_URL = "https://%s.my.salesforce.com" % self.client
        url = BASE_URL + "/services/data/v44.0/query/"
        if not next_records_url:
            r = requests.get(url, params=params, headers=headers).json()
        else:
            r = requests.get(BASE_URL + next_records_url, headers=headers).json()
        result = result + r["records"]
        next_records_url = r.get('nextRecordsUrl')
        i = 1
        while i < batch_size and next_records_url:
            r = requests.get(BASE_URL + next_records_url, headers=headers).json()
            result = result + r["records"]
            next_records_url = r.get('nextRecordsUrl')
            i = i + 1
        return {"records": result, "object": object_name, "next_records_url": next_records_url}

    def process_data(self, raw_data):
        object_row = []
        object_description = self.describe_objects(raw_data.get("object"))
        for r in raw_data["records"]:
            _object = dict()
            for o in object_description:
                if type(r.get(o)) != dict:
                    _object[o.lower()] = r.get(o)
                else:
                    _object[o.lower()] = None
            object_row.append(_object)
        return object_row

    def get_column_names(self, data):
        column_list = []
        for d in data:
            for c in d.keys():
                if c not in column_list:
                    column_list.append(c)
        return column_list

    def send_data(self, data, object_name, datamart, since=True):
        column_names = self.get_column_names(data)
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r[c] for c in column_names] for r in data],
            "table_name": self.schema_prefix + object_name}
        if not since:
            datamart.send_data(
                data=data_to_send,
                replace=True)
        else:
            datamart.execute_query(
                "drop table if exists %(schema_name)s.%(table_name)s_temp cascade; "
                "CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s where id= 1)" % {
                    'table_name': object_name,
                    "schema_name": self.schema_prefix})
            data_to_send_temp = {
                "columns_name": column_names,
                "rows": [[r[c] for c in column_names] for r in data],
                "table_name": self.schema_prefix + object_name + "_temp"}
            datamart.send_data(
                data=data_to_send_temp,
                replace=False)
            self._clean(datamart, object_name)

    def _clean(self, datamart, object_name):
        selecting_id = 'id'
        cleaning_query = """
            DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
            INSERT INTO %(schema_name)s.%(table_name)s (SELECT * FROM %(schema_name)s.%(table_name)s_temp);
            """ % {"table_name": object_name,
                   "schema_name": self.schema_prefix,
                   "id": selecting_id}
        datamart.execute_query(cleaning_query)

    def main(self, since_start=False, batchsize=100):

        datamart = RedDBStream(
            instance_name=self.client,
            client_id=self.clientid
        )

        for p in self.get_objects():
            print(p)
            since = None
            if since_start == False:
                since = start_end_from_last_call(table=self.schema_prefix + p.lower(),
                                                 datamart=datamart, last_n_days=3)
            print(since)
            t0 = time.time()
            raw_data = self.execute_query(p, batchsize, since)
            t1 = time.time()
            next_records_url = raw_data.get("next_records_url")
            data = self.process_data(raw_data)
            t2 = time.time()
            self.send_data(data, p, datamart, since)
            t3 = time.time()
            print(t1 - t0)
            print(t2 - t1)
            print(t3 - t2)
            print("END")
            while next_records_url:
                raw_data = self.execute_query(p, batchsize, next_records_url, since)
                next_records_url = raw_data.get("next_records_url")
                data = self.process_data(raw_data)
                self.send_data(data, p, datamart, since)
