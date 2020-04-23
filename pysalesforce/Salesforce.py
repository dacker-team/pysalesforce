from datetime import date, datetime, timedelta

import psycopg2
import pyodbc as pyodbc
import yaml
import requests
from pysalesforce.auth import get_token_and_base_url
from pysalesforce.date import start_end_from_last_call
from pysalesforce.useful import process_data, get_column_names, _clean, create_temp_table, send_temp_data


class Salesforce:
    # l'api version par défaut on met 44 non ? ou autre ? mais avec None ca marche pas non ? faudrait voir aussi ce que ca
    # donne si on fait un call avec la mauvaise version et traiter proprement l'erreur pour que ce soit forcément explicite
    # pour MH on en n'a pas besoin donc je laisse par défaut None, je referai le test avec Ledger et je regarde l'erreur que ça pourrait donner

    # Base_url vient de la réponse de l'access_token
    # login=False correspont a auth_url avec test, login=True correspond à auth_url avec login

    def __init__(self, var_env_key, dbstream, config_file_path, salesforce_test_instance=False, api_version=None):
        self.var_env_key = var_env_key
        self.dbstream = dbstream
        self.config_file_path = config_file_path
        self.salesforce_test_instance = salesforce_test_instance
        [self.access_token, self.base_url] = get_token_and_base_url(var_env_key, self.salesforce_test_instance)
        self.api_version = api_version

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("objects")

    def get_endpoint(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("endpoints")

    def get_schema_prefix(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("schema_prefix")

    def describe_objects(self, object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = self.base_url + "/services/data/%s/sobjects/%s/describe/" % (self.api_version, object_name)
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["fields"]]

    def query(self, object_name, since):
        fields = self.describe_objects(object_name)
        where_clause = ""
        if since:
            if 'LastModifiedDate' in fields:
                where_clause = " where lastmodifieddate >= %s" % since
        query = 'select '
        for p in fields:
            query += p + ','
        query = query[:-1]
        query += ' from ' + object_name + where_clause
        return query

    def execute_query(self, object_name, batch_size, since, next_records_url=None):
        result = []
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        params = {
            "q": self.query(object_name.get('name'), since)
        }
        url = self.base_url + "/services/data/%s/query/" % self.api_version
        if not next_records_url:
            r = requests.get(url, params=params, headers=headers).json()
        else:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
        result = result + r.get("records")
        next_records_url = r.get('nextRecordsUrl')
        i = 1
        while i < batch_size and next_records_url:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
            result = result + r["records"]
            next_records_url = r.get('nextRecordsUrl')
            i = i + 1
        return {"records": result, "object": object_name, "next_records_url": r.get('nextRecordsUrl')}

    def retrieve_endpoint(self, endpoint, since=None):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        params = {}
        if since:
            params = {'lastModificationDate': since}
        url = self.base_url + "/services/apexrest/%s" % endpoint
        r = requests.get(url, headers=headers, params=params).json()
        return r

    def main(self, object, schema, since_start=False, batchsize=100):
        print('Starting ' + object.get('name'))
        create_temp_table(self.dbstream,schema,object.get('table'))
        since = None
        next_url = None
        if not since_start:
            since = start_end_from_last_call(self,object)
        if object.get("endpoint") == True:
            raw_data = self.retrieve_endpoint(object.get('name'), since)
            data = process_data(raw_data)
        else:
            raw_data = self.execute_query(object, batchsize, since)
            next_url = raw_data.get("next_records_url")
            data = process_data(raw_data["records"])
        send_temp_data(self.dbstream,data,schema, object.get('table'))
        while next_url:
            raw_data = self.execute_query(object, batchsize, next_records_url=next_url, since=None)
            next_url = raw_data.get("next_records_url")
            data = process_data(raw_data["records"])
            send_temp_data(self.dbstream,data,schema,object.get('table'))
        print('Ended ' + object.get('name'))
        _clean(self.dbstream,schema,object.get('table'))
