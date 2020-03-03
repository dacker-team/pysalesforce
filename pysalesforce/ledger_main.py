import pysalesforce
from pysalesforce.Salesforce import Salesforce


def ledger_main():
    s=Salesforce("LEDGER")
    batchsize=1
    for p in s.get_objects():
        print(p)
        s.execute_query(p,batchsize)
        raw_data=s.execute_query(p,batchsize)
        data=s.process_data(raw_data)
        print(s.get_column_names(data))



