from dateutil.utils import today


def process_data(raw_data, remove_columns=None, imported_at=True):
    object_row = []
    for r in raw_data:
        _object = dict()
        if remove_columns:
            for c in remove_columns:
                r.pop(c, None)
        for o in r.keys():
            if type(r.get(o)) == dict:
                _object[o.lower()] = str(r.get(o))
            else:
                _object[o.lower()] = r.get(o)
        if imported_at:
            _object['imported_at'] = today()
        object_row.append(_object)
    return object_row


def get_column_names(data):
    column_list = []
    for d in data:
        for c in d.keys():
            if c not in column_list:
                column_list.append(c)
    return column_list


def send_temp_data(datamart, data, schema_prefix, table, column_names):
    data_to_send = {
        "columns_name": column_names,
        "rows": [[r[c] for c in column_names] for r in data],
        "table_name": schema_prefix + '.' + table + '_temp'}
    datamart.send_data(
        data=data_to_send,
        other_table_to_update=schema_prefix + '.' + table,
        replace=False)


def _clean(datamart, schema_prefix, table):
    selecting_id = 'id'
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
