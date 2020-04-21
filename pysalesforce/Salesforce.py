from datetime import time

import psycopg2
import pyodbc as pyodbc
import yaml
import requests
from pysalesforce.auth import get_access_token
from pysalesforce.date import start_end_from_last_call


class Salesforce:
    # j'ai renomé client en var_env_key
    # est ce qu'on mettrait pas le schema prefix dans le config file ?
    # le client id pas besoin il est déjà dans le datamart. on y accede en faisant datamart.client_id
    # j'appelerai bien datamart dbstream plutot d'ailleurs
    # l'auth url c'est top mais ca pourrait pas être tout simplement un boolean dev_instance par défaut à False ? Il y a que 2 urls je crois sur Salesforce pour l'auth --> j'ai vu qu'il fallait gérer ca plus proprement dans auth
    # la base url on peut la récupérer avec l'access token non ?
    # l'api version par défaut on met 44 non ? ou autre ? mais avec None ca marche pas non ? faudrait voir aussi ce que ca
    # donne si on fait un call avec la mauvaise version et traiter proprement l'erreur pour que ce soit forcément explicite

    def __init__(self, var_env_key, dbstream, config_file_path, schema_prefix, auth_url, base_url, api_version=None):
        self.dbstream = dbstream
        self.config_file_path = config_file_path
        self.auth_url = auth_url
        self.access_token = get_access_token(var_env_key, self.auth_url)
        self.schema_prefix = schema_prefix
        self.base_url = base_url
        self.api_version = api_version

    def get_objects(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("objects")

    def get_endpoint(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
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

    def retrieve_endpoint(self, endpoint, since=None):
        # Ca il faut le gérer plus proprement, avec les params de requests, je m'explique:
        # quand tu fais une requete sur un url www.test.com en ajoutant ?key=value derriere donc:
        # www.test.com?key=value
        # ca revient à utiliser requests avec url=www.test.com et params={key:value}
        # et donc à gérer les params derriere l'url proprement dans un dictionnaire

        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        if not since:
            condition = ''
        else:
            condition = '?lastModificationDate=%s' % since

        url = self.base_url + "/services/apexrest/%s" % endpoint + condition
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
        # pourquoi la fonction au dessus a besoin de describe objets et pas celle ci ?
        # là on pourrait la sortir de la classe

        object_row = []
        for r in raw_data:
            _object = dict()
            for k in r.keys():
                _object[k] = r.get(k)
            object_row.append(_object)
        return object_row

    def get_column_names(self, data):
        # IDEM

        column_list = []
        for d in data:
            for c in d.keys():
                if c not in column_list:
                    column_list.append(c)
        return column_list

    def create_temp_table(self, table):
        # C'est normal que cette fonction crée pas une exception, juste un print ?

        try:
            self.dbstream.execute_query(
                '''drop table if exists %(schema_name)s.%(table_name)s_temp cascade; ''' +
                '''CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s 
                where id= 1)''' % {
                    'table_name': table,
                    "schema_name": self.schema_prefix})
        except:
            print('temp_table not created')

    def send_temp_data(self, data, table):
        column_names = self.get_column_names(data)
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r[c] for c in column_names] for r in data],
            "table_name": self.schema_prefix + '.' + table + '_temp'}
        print('True')
        self.dbstream.send_data(
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
            self.dbstream.execute_query(cleaning_query)
            print('cleaned')
        except psycopg2.errors.UndefinedTable:
            print('destination table does not exist, will be created')
            insert_query = """
            DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
            CREATE TABLE %(schema_name)s.%(table_name)s as SELECT * FROM %(schema_name)s.%(table_name)s_temp;
            """ % {"table_name": table,
                   "schema_name": self.schema_prefix}
            self.dbstream.execute_query(insert_query)
        except pyodbc.ProgrammingError:
            print('destination table does not exist, will be created')
            insert_query = """
                        DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
                        SELECT * into %(schema_name)s.%(table_name)s from %(schema_name)s.%(table_name)s_temp;
                        """ % {"table_name": table,
                               "schema_name": self.schema_prefix}
            self.dbstream.execute_query(insert_query)

    def main(self, since_start=False, batchsize=100, endpoint=False):
        # OK 2 choses principale :
        # 1) j'ai limpression que pas mal de choses se répètent dans ce main, et surtout je comprends pas pourquoi
        # les next_url sont gérés là ET dans execute_query non dans le cas de endpoint False ?
        # 2) De manière je pense que c'est mieux de gérer les choses en terme d'objet, qui par défaut n'utilisent pas un endpoint particulier,
        # mais peuvent le faire. Dans le fichier de config "endpoint" serait plutot un attribut de l'objet plutot que l'objet un attribut de endpoint
        # ce qui fait que le parametre du main est plutot le nom d'objet (optionnel, sinon tout le fichier de config est parcouru) et
        # à chaque fois par défaut execute_query est utilisé sauf si l'objet demande un endpoint

        if endpoint == False:
            for p in self.get_objects():
                print('Starting ' + p.get('name'))
                self.create_temp_table(p.get('table'))
                since = None
                if not since_start:
                    since = start_end_from_last_call(self, p)
                raw_data = self.execute_query(p, batchsize, since)
                next_url = raw_data.get("next_records_url")
                data = self.process_data(raw_data)
                self.send_temp_data(data, p.get('table'))
                while next_url:
                    raw_data = self.execute_query(p, batchsize, next_records_url=next_url, since=None)
                    next_url = raw_data.get("next_records_url")
                    data = self.process_data(raw_data)
                    self.send_temp_data(data, p)
                print('Ended ' + p.get('name'))
                self._clean(p.get('table'))
        else:
            for p in self.get_endpoint():
                print('Starting ' + p.get('name'))
                self.create_temp_table(p.get('table'))
                since = None
                if not since_start:
                    since = time.yesterday()
                raw_data = self.retrieve_endpoint(p.get('name'), since)
                data = self.process_data_endpoint(raw_data)
                print(data)
                self.send_temp_data(data, p.get('table'))
            print('Ended ' + p.get('name'))
            self._clean(p.get('table'))
