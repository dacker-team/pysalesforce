import datetime
import io
import json
import uuid
import pandas as pd
import yaml
import requests
from pysalesforce.auth import get_token_and_base_url
from pysalesforce.useful import process_data, get_column_names, _clean, send_temp_data


class Salesforce:
    # >>>Si on fait un call avec la mauvaise version et traiter proprement l'erreur pour que ce soit forcément explicite

    def __init__(self, var_env_key, dbstream, config_file_path, salesforce_test_instance=False, api_version=None):
        self.var_env_key = var_env_key
        self.dbstream = dbstream
        self.config_file_path = config_file_path
        self.salesforce_test_instance = salesforce_test_instance
        self.access_token, self.base_url = get_token_and_base_url(var_env_key, self.salesforce_test_instance)
        self.api_version = api_version
        self.objects = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader).get('objects')
        self.schema_prefix = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader).get("schema_prefix")

    def get_endpoint(self):
        config = yaml.load(open(self.config_file_path), Loader=yaml.FullLoader)
        return config.get("endpoints")

    def get_table(self, _object_key):
        _object = self.objects[_object_key]
        if not _object.get('table'):
            return _object_key.lower() + 's'
        return _object.get('table')

    def describe_objects(self, object_name):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        url = self.base_url + "/services/data/%s/sobjects/%s/describe/" % (self.api_version, object_name)
        result = requests.get(url, headers=headers).json()

        fields = []
        for r in result["fields"]:
            if r["type"] not in ["address", "location"]:
                fields.append(r["name"])
        return fields

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

    def launch_job(self, query, batch_id):
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Content-type': 'application/json'
        }

        url = self.base_url + "/services/data/%s/jobs/query" % self.api_version
        data = {
            "operation": "queryAll",
            "query": query
        }
        r = requests.post(url, data=json.dumps(data), headers=headers).json()
        if isinstance(r, list):
            raise Exception("Error : %s" % r)
        r["batch_id"] = batch_id
        r["fetched_at"] = None
        self.dbstream.send(
            replace=False,
            data={
                "data": [r],
                "table_name": "%s.%s" % (self.schema_prefix, "_jobs")
            }
        )

    def update_jobs_status(self,
                           stop_if_one_is_completed=False,
                           batch_id=None):
        jobs_query = """
        select * 
        from %s._jobs 
        where state in ('UploadComplete', 'InProgress')
        and %s 
        order by createddate
        """ % (self.schema_prefix, ("batch_id='%s'" % batch_id) if batch_id else "1=1")

        jobs = self.dbstream.execute_query(jobs_query)

        for job in jobs:
            headers = {
                "Authorization": "Bearer %s" % self.access_token
            }

            url = self.base_url + "/services/data/%s/jobs/query/%s" % (self.api_version, job["id"])

            r = requests.get(url, headers=headers).json()
            self.dbstream.execute_query("delete from %s._jobs where id='%s'" % (self.schema_prefix, job["id"]))
            r["batch_id"] = job["batch_id"]
            r["fetched_at"] = None
            self.dbstream.send(
                replace=False,
                data={
                    "data": [r],
                    "table_name": "%s.%s" % (self.schema_prefix, "_jobs")
                }
            )
            if stop_if_one_is_completed and r["state"] == "JobComplete":
                return job["id"], job["object"].lower()
        return None, None

    def get_data_completed_job(self, job_id, locator=None):
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            "Accept": "text/csv"
        }
        url = self.base_url + "/services/data/%s/jobs/query/%s/results" % (self.api_version, job_id)
        params = {"maxRecords": 10000}
        if locator:
            params.update({"locator": locator})
        r = requests.get(url, params=params, headers=headers)
        locator = r.headers["Sforce-Locator"]
        df = pd.read_csv(io.StringIO(r.text))
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records"), locator

    def process_bulk_data(self, _object_key, job_id):
        _object = self.objects[_object_key]
        schema = self.schema_prefix
        table = self.get_table(_object_key)
        dbstream = self.dbstream

        raw_data, locator = self.get_data_completed_job(job_id)
        data = process_data(
            raw_data=raw_data,
            remove_columns=_object.get('remove_columns'),
            imported_at=_object.get('imported_at')
        )
        columns = get_column_names(data)
        dbstream.send_with_temp_table(data, columns, 'id', schema, table)

        while locator != "null":
            raw_data, locator = self.get_data_completed_job(job_id, locator)
            data = process_data(
                raw_data=raw_data,
                remove_columns=_object.get('remove_columns'),
                imported_at=_object.get('imported_at')
            )
            dbstream.send_with_temp_table(data, columns, 'id', schema, table)
        self.dbstream.execute_query("update %s._jobs set fetched_at='%s' where id='%s'" % (
            schema, datetime.datetime.now(), job_id)
                                    )

    def main_bulk(self, jobs_to_launch, batch_id=None, timeout=None):
        if not batch_id:
            batch_id = str(uuid.uuid4())
        print(datetime.datetime.now())
        if not batch_id:
            for job in jobs_to_launch:
                self.launch_job(self.query(job["object"], job["since"]), batch_id)
                print("%s launched job %s " % (datetime.datetime.now(), job["object"]))

        job_id, _object = self.update_jobs_status(batch_id=batch_id, stop_if_one_is_completed=True)
        while job_id is not None:
            self.process_bulk_data(_object, job_id)
            job_id, _object = self.update_jobs_status(batch_id=batch_id, stop_if_one_is_completed=True)
            print("%s job processed %s " % (datetime.datetime.now(), job_id))
        objects_not_loaded = self.dbstream.execute_query(
            """
            select
                object 
            from %s._jobs
            where batch_id='%s'
            and  fetched_at is null 
            """ % (self.schema_prefix, batch_id)
        )
        if objects_not_loaded:
            raise Exception(
                "This Salesforce objects was not fetched in batch_id %s : %s" % (batch_id, ",".join([k["object"] for k in objects_not_loaded])))

    def execute_query(self, _object_key, batch_size, since, next_records_url=None):
        result = []
        headers = {
            "Authorization": "Bearer %s" % self.access_token,
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        params = {
            "q": self.query(_object_key, since)
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
        return {"records": result, "object": _object_key, "next_records_url": r.get('nextRecordsUrl')}

    def retrieve_endpoint(self, endpoint, since=None, next_url=None):
        headers = {
            "Authorization": "Bearer %s" % self.access_token
        }
        if next_url:
            return requests.get(next_url, headers=headers).json()

        params = {}
        if since:
            params = {'lastModificationDate': since}
        url = "%s/services/apexrest/%s" % (self.base_url, endpoint)
        return requests.get(url, headers=headers, params=params).json()

    def process_endpoint_data(self, _object, _object_key, table, since=None, next_url=None):
        raw_data = self.retrieve_endpoint(_object_key, since=since, next_url=next_url)
        data = process_data(
            raw_data=raw_data[table],
            remove_columns=_object.get('remove_columns'),
            imported_at=_object.get('imported_at')
        )
        return data, raw_data.get("nextPageURL")

    def process_object_data(self, _object, _object_key, batchsize, since, next_url=None):
        raw_data = self.execute_query(_object_key, batchsize, since=since, next_records_url=next_url)
        data = process_data(
            raw_data=raw_data["records"],
            remove_columns=_object.get('remove_columns'),
            imported_at=_object.get('imported_at')
        )
        return data, raw_data.get("next_records_url")

    def main(self, _object_key, since=None, batchsize=10):
        print('Starting ' + _object_key)

        _object = self.objects[_object_key]
        schema = self.schema_prefix
        table = self.get_table(_object_key)
        dbstream = self.dbstream
        next_url = None

        if _object.get("endpoint"):
            data, next_url = self.process_endpoint_data(
                _object=_object,
                _object_key=_object_key,
                table=table, since=since,
                next_url=next_url
            )
            columns = get_column_names(data)
            dbstream.send_with_temp_table(data, columns, 'id', schema, table)
            while next_url:
                data, next_url = self.process_endpoint_data(
                    _object=_object,
                    _object_key=_object_key,
                    table=table,
                    since=since,
                    next_url=next_url
                )
                dbstream.send_with_temp_table(data, columns, 'id', schema, table)

        else:
            data, next_url = self.process_object_data(
                _object=_object,
                _object_key=_object_key,
                batchsize=batchsize,
                since=since,
                next_url=next_url
            )
            columns = get_column_names(data)
            dbstream.send_with_temp_table(data, columns, 'id', schema, table)
            while next_url:
                data, next_url = self.process_object_data(
                    _object=_object,
                    _object_key=_object_key,
                    batchsize=batchsize,
                    next_url=next_url,
                    since=since
                )
                dbstream.send_with_temp_table(data, columns, 'id', schema, table)

        print('Ended ' + _object_key)
