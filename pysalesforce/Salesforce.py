from datetime import date, datetime, timedelta

import psycopg2
import pyodbc as pyodbc
import yaml
import requests
from pysalesforce.auth import get_token_and_base_url
from pysalesforce.date import start_end_from_last_call


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

    def process_data(self, raw_data):
        object_row = []
        for r in raw_data:
            _object = dict()
            for o in r.keys():
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

    def create_temp_table(self, schema_prefix, table):
        # C'est normal que cette fonction crée pas une exception, juste un print ?
        try:
            self.dbstream.execute_query(
                '''drop table if exists %(schema_name)s.%(table_name)s_temp cascade; ''' +
                '''CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s 
                where id= 1)''' % {
                    'table_name': table,
                    "schema_name": schema_prefix})
        except:
            print('temp_table not created')

    def send_temp_data(self, data, schema_prefix, table):
        column_names = self.get_column_names(data)
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r[c] for c in column_names] for r in data],
            "table_name": schema_prefix + '.' + table + '_temp'}
        self.dbstream.send_data(
            data=data_to_send,
            replace=False)

    def _clean(self,schema_prefix, table):
        selecting_id = 'id'
        try:
            print('trying to clean')
            cleaning_query = """
                    DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
                    INSERT INTO %(schema_name)s.%(table_name)s 
                    SELECT * FROM %(schema_name)s.%(table_name)s_temp;
                    DELETE FROM %(schema_name)s.%(table_name)s_temp;
                    """ % {"table_name": table,
                           "schema_name": schema_prefix,
                           "id": selecting_id}
            self.dbstream.execute_query(cleaning_query)
            print('cleaned')
        except psycopg2.errors.UndefinedTable:
            print('destination table does not exist, will be created')
            insert_query = """
            DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
            CREATE TABLE %(schema_name)s.%(table_name)s as SELECT * FROM %(schema_name)s.%(table_name)s_temp;
            """ % {"table_name": table,
                   "schema_name": schema_prefix}
            self.dbstream.execute_query(insert_query)
        except pyodbc.ProgrammingError:
            print('destination table does not exist, will be created')
            insert_query = """
                        DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
                        SELECT * into %(schema_name)s.%(table_name)s from %(schema_name)s.%(table_name)s_temp;
                        """ % {"table_name": table,
                               "schema_name": schema_prefix}
            self.dbstream.execute_query(insert_query)

    def main(self, since_start=False, batchsize=100, endpoint=False):
        # OK 2 choses principale :
        # 1) j'ai limpression que pas mal de choses se répètent dans ce main, et surtout je comprends pas pourquoi
        # les next_url sont gérés là ET dans execute_query non dans le cas de endpoint False ?
        # 2) De manière je pense que c'est mieux de gérer les choses en terme d'objet, qui par défaut n'utilisent pas un endpoint particulier,
        # mais peuvent le faire. Dans le fichier de config "endpoint" serait plutot un attribut de l'objet plutot que l'objet un attribut de endpoint
        # ce qui fait que le parametre du main est plutot le nom d'objet (optionnel, sinon tout le fichier de config est parcouru) et
        # à chaque fois par défaut execute_query est utilisé sauf si l'objet demande un endpoint
        schema=self.get_schema_prefix()
        if endpoint == False:
            for p in self.get_objects():
                print('Starting ' + p.get('name'))
                self.create_temp_table(schema,p.get('table'))
                since = None
                if not since_start:
                    since = start_end_from_last_call(self, p)
                raw_data = self.execute_query(p, batchsize, since)
                next_url = raw_data.get("next_records_url")
                data = self.process_data(raw_data["records"])
                self.send_temp_data(data,schema, p.get('table'))
                while next_url:
                    raw_data = self.execute_query(p, batchsize, next_records_url=next_url, since=None)
                    next_url = raw_data.get("next_records_url")
                    data = self.process_data(raw_data["records"])
                    self.send_temp_data(data,schema,p.get('table'))
                print('Ended ' + p.get('name'))
                self._clean(schema,p.get('table'))
        else:
            for p in self.get_endpoint():
                print('Starting ' + p.get('name'))
                self.create_temp_table(schema,p.get('table'))
                since = None
                if not since_start:
                    since = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
                raw_data = self.retrieve_endpoint(p.get('name'), since)
                data = self.process_data(raw_data)
                print(data)
                self.send_temp_data(data,schema, p.get('table'))
            print('Ended ' + p.get('name'))
            self._clean(schema,p.get('table'))
