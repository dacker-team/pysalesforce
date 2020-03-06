import datetime
import psycopg2
import pyred

from pyred import RedDBStream
from dateutil.parser import parse



def start_end_from_last_call(table, datamart, last_n_days=None):
    try:
        query = "SELECT MAX(lastmodifieddate) as max FROM %s" % (table)
        result_query = pyred.RedDBStream.execute_query(datamart, query)
        max = result_query[0]["max"]
        start=max
        if last_n_days:
            start = max + datetime.timedelta(days=-last_n_days)
            print(start)
    except (IndexError, TypeError, psycopg2.ProgrammingError) as e:
        start = None
        pass
    return str(start.isoformat())+"Z"

