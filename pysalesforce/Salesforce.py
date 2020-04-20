from datetime import time

import psycopg2
import pyodbc as pyodbc
import yaml
import requests
from pysalesforce.auth import get_access_token
from pysalesforce.date import start_end_from_last_call


class Salesforce:
    def __init__(self, client, clientid, datamart, config_file_path, schema_prefix, auth_url, base_url, api_version=None):
        self.client = client
        self.clientid = clientid
        self.datamart = datamart
        self.config_file_path = config_file_path
        self.auth_url=auth_url
        self.access_token = get_access_token(client, self.auth_url)
        self.schema_prefix = schema_prefix
        self.base_url = base_url
        self.api_version = api_version

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("objects")

    def get_endpoint(self):
        config=yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("endpoints")

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
            "q": self.query(object_name.get('name'), since)
        }
        url = self.base_url + "/services/data/%s/query/" % self.api_version
        if not next_records_url:
            r = requests.get(url, params=params, headers=headers).json()
        else:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
        print(r)
        result = result + r.get("records")
        next_records_url = r.get('nextRecordsUrl')
        i = 1
        while i < batch_size and next_records_url:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
            result = result + r["records"]
            next_records_url = r.get('nextRecordsUrl')
            i = i + 1
        return {"records": result, "object": object_name, "next_records_url": r.get('nextRecordsUrl')}

    def retrieve_endpoint(self, endpoint,since=None):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        if not since:
            condition=''
        else:
            condition='?lastModificationDate=%s' %since
        url = self.base_url + "/services/apexrest/%s" %endpoint+condition
        r = requests.get(url, headers=headers).json()
        return r

    def process_data(self, raw_data):
        object_row = []
        object_description = self.describe_objects(raw_data.get("object")["name"])
        for r in raw_data["records"]:
            _object = dict()
            for o in object_description:
                if type(r.get(o)) == dict:
                    _object[o.lower()] = str(r.get(o))
                else:
                    _object[o.lower()] = r.get(o)
            object_row.append(_object)
        return object_row

    def process_data_endpoint(self, raw_data):
        object_row = []
        for r in raw_data:
            _object = dict()
            for k in r.keys():
                _object[k]=r.get(k)
            object_row.append(_object)
        return object_row

    def get_column_names(self, data):
        column_list = []
        for d in data:
            for c in d.keys():
                if c not in column_list:
                    column_list.append(c)
        return column_list

    def create_temp_table(self, table):
        try:
            self.datamart.execute_query(
                '''drop table if exists %(schema_name)s.%(table_name)s_temp cascade; ''' +
                '''CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s 
                where id= 1)''' % {
                    'table_name': table,
                    "schema_name": self.schema_prefix})
        except:
            print('temp_table not created')

    def send_temp_data(self, data,table):
        column_names = self.get_column_names(data)
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r[c] for c in column_names] for r in data],
            "table_name": self.schema_prefix + '.' + table + '_temp'}
        print('True')
        self.datamart.send_data(
            data=data_to_send,
            replace=False)


    def _clean(self, table):
        selecting_id = 'id'
        try:
            print('trying to clean')
            cleaning_query = """
                    DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
                    INSERT INTO %(schema_name)s.%(table_name)s 
                    SELECT * FROM %(schema_name)s.%(table_name)s_temp;
                    DELETE FROM %(schema_name)s.%(table_name)s_temp;
                    """ % {"table_name": table,
                           "schema_name": self.schema_prefix,
                           "id": selecting_id}
            self.datamart.execute_query(cleaning_query)
            print('cleaned')
        except psycopg2.errors.UndefinedTable :
            print('destination table does not exist, will be created')
            insert_query = """
            DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
            CREATE TABLE %(schema_name)s.%(table_name)s as SELECT * FROM %(schema_name)s.%(table_name)s_temp;
            """% {"table_name": table,
                           "schema_name": self.schema_prefix}
            self.datamart.execute_query(insert_query)
        except pyodbc.ProgrammingError:
            print('destination table does not exist, will be created')
            insert_query = """
                        DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
                        SELECT * into %(schema_name)s.%(table_name)s from %(schema_name)s.%(table_name)s_temp;
                        """ % {"table_name": table,
                               "schema_name": self.schema_prefix}
            self.datamart.execute_query(insert_query)


    def main(self, since_start=False, batchsize=100, endpoint=False):
        if endpoint==False:
            for p in self.get_objects():
                print('Starting ' + p.get('name'))
                self.create_temp_table(p.get('table'))
                since = None
                if not since_start:
                    since=start_end_from_last_call(self,p)
                raw_data = self.execute_query(p, batchsize, since)
                next_url = raw_data.get("next_records_url")
                data = self.process_data(raw_data)
                self.send_temp_data(data, p.get('table'))
                while next_url:
                    raw_data = self.execute_query(p, batchsize, next_records_url=next_url, since=None)
                    next_url=raw_data.get("next_records_url")
                    data = self.process_data(raw_data)
                    self.send_temp_data(data, p)
                print('Ended ' + p.get('name'))
                self._clean(p.get('table'))
        else:
            for p in self.get_endpoint():
                print('Starting ' + p.get('name'))
                self.create_temp_table(p.get('table'))
                since=None
                if not since_start:
                    since=time.yesterday()
                raw_data=self.retrieve_endpoint(p.get('name'),since)
                data=self.process_data_endpoint(raw_data)
                print(data)
                self.send_temp_data(data, p.get('table'))
            print('Ended ' + p.get('name'))
            self._clean(p.get('table'))



