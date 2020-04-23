import psycopg2
import pyodbc


def process_data(raw_data):
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


def get_column_names(data):
    column_list = []
    for d in data:
        for c in d.keys():
            if c not in column_list:
                column_list.append(c)
    return column_list

def create_temp_table(datamart, schema_prefix, table):
    # C'est normal que cette fonction crée pas une exception, juste un print ?
    try:
        datamart.execute_query(
            '''drop table if exists %(schema_name)s.%(table_name)s_temp cascade; ''' +
            '''CREATE TABLE %(schema_name)s.%(table_name)s_temp AS (SELECT * FROM %(schema_name)s.%(table_name)s 
            where id= 1)''' % {
                'table_name': table,
                "schema_name": schema_prefix})
    except:
        print('temp_table not created')

def send_temp_data(datamart, data, schema_prefix, table):
    column_names = get_column_names(data)
    data_to_send = {
        "columns_name": column_names,
        "rows": [[r[c] for c in column_names] for r in data],
        "table_name": schema_prefix + '.' + table + '_temp'}
    datamart.send_data(
        data=data_to_send,
        replace=False)

def _clean(datamart,schema_prefix, table):
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
        datamart.execute_query(cleaning_query)
        print('cleaned')
    except psycopg2.errors.UndefinedTable:
        print('destination table does not exist, will be created')
        insert_query = """
        DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
        CREATE TABLE %(schema_name)s.%(table_name)s as SELECT * FROM %(schema_name)s.%(table_name)s_temp;
        """ % {"table_name": table,
               "schema_name": schema_prefix}
        datamart.execute_query(insert_query)
    except pyodbc.ProgrammingError:
        print('destination table does not exist, will be created')
        insert_query = """
                    DROP TABLE IF EXISTS %(schema_name)s.%(table_name)s;
                    SELECT * into %(schema_name)s.%(table_name)s from %(schema_name)s.%(table_name)s_temp;
                    """ % {"table_name": table,
                           "schema_name": schema_prefix}
        datamart.execute_query(insert_query)