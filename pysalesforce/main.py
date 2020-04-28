from pyred import RedDBStream

from pysalesforce.Salesforce import Salesforce

var_env_key = 'XXXXX'
datamart = RedDBStream(
    'XXXXX',
    client_id=1234
)
config_file_path = 'XXXXXXXXX'
api_version = 'v44.0'

s = Salesforce(var_env_key, datamart, config_file_path, salesforce_test_instance=False, api_version=api_version)
schema = s.get_schema_prefix()
objects = s.get_objects()
for o in objects:
    s.main(o, schema ,since_start=True)
