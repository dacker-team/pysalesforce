import yaml
import requests
from pysalesforce.auth import get_access_token
from pysalesforce.date import start_end_from_last_call


class Salesforce:
    def __init__(self, client, clientid, datamart, config_file_path, schema_prefix, base_url, api_version=None):
        self.client = client
        self.clientid = clientid
        self.datamart = datamart
        self.config_file_path = config_file_path
        self.access_token = get_access_token(client)
        self.schema_prefix = schema_prefix
        self.base_url = base_url
        self.api_version = api_version

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("objects")

    def describe_objects(self, object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = self.base_url + "/services/data/%s/sobjects/%s/describe" % (self.api_version, object_name)
        result = requests.get(url, headers=headers).json()
        return [r["name"] for r in result["fields"]]

    def query(self, object_name, since):
        fields = self.describe_objects(object_name)
        where_clause = ""
        if since and 'lastmodifieddate' in fields:
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
            "q": self.query(object_name, since)
        }
        url = self.base_url + "/services/data/%s/query/" % self.api_version
        if not next_records_url:
            r = requests.get(url, params=params, headers=headers).json()
        else:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
        result = result + r["records"]
        next_records_url = r.get('nextRecordsUrl')
        i = 1
        while i < batch_size and next_records_url:
            r = requests.get(self.base_url + next_records_url, headers=headers).json()
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
                if type(r.get(o)) == dict:
                    _object[o.lower()] = str(r.get(o))
                else:
                    _object[o.lower()] = r.get(o)
            object_row.append(_object)
        return object_row

    def get_column_names(self, data):
        column_list = []
        for d in data:
            for c in d.keys():
                if c not in column_list:
                    column_list.append(c)
        return column_list

    def create_temp_table(self, o):
        try:
            self.datamart.execute_query(
                '''drop table if exists %(schema_name)s.%(table_name)s_temp cascade; ''' +
                '''CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s 
                where id= 1)''' % {
                    'table_name': o.get('table'),
                    "schema_name": self.schema_prefix})
        except:
            pass

    def send_temp_data(self, data, o):
        column_names = self.get_column_names(data)
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r[c] for c in column_names] for r in data],
            "table_name": self.schema_prefix + '.' + o.get('table') + '_temp'}
        self.datamart.send_data(
            data=data_to_send,
            replace=False)


def _clean(self, object_name):
    selecting_id = 'id'
    try:
        cleaning_query = """
                DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
                INSERT INTO %(schema_name)s.%(table_name)s (SELECT * FROM %(schema_name)s.%(table_name)s_temp);
                """ % {"table_name": object_name,
                       "schema_name": self.schema_prefix,
                       "id": selecting_id}
        self.datamart.execute_query(cleaning_query)
    except:
        insert_query = '''CREATE TABLE%(schema_name)s.%(table_name)s as (SELECT * FROM %(schema_name)s.%(table_name)s_temp)'''
        self.datamart.execute_query(insert_query)


def main(self, since_start=False, batchsize=100):
    for p in self.get_objects():
        print('Starting ' + p.get('api_name'))
        self.create_temp_table(p)
        since = None
        if not since_start:
            since = start_end_from_last_call(self, p)
        raw_data = self.execute_query(p, batchsize, since)
        next_records_url = raw_data.get("next_records_url")
        data = self.process_data(raw_data)
        self.send_temp_data(data, p)
        while next_records_url:
            raw_data = self.execute_query(p, batchsize, next_records_url, since)
            next_records_url = raw_data.get("next_records_url")
            data = self.process_data(raw_data)
            self.send_temp_data(data, p)
        self._clean(p)
        print('End')
