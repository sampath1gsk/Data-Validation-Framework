import os
# Need to install first time manually
# os.system('install_packages.bat')
import connections
import pandas as pd
import io
import csv
import math
import sys
import itertools
import warnings
from datetime import datetime
from input_server_details import global_result
# Suppress specific warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", FutureWarning)

def get_datatype(connection, db, schema, table_name):
    column_datatypes = pd.read_sql(f" SELECT UPPER(COLUMN_NAME) as COLUMN_NAME, UPPER(DATA_TYPE) as DATA_TYPE"
                                   f" FROM {db}.INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME= '{table_name}' "
                                   f" AND TABLE_SCHEMA ='{schema}' ", connection)
    column_datatypes = column_datatypes.set_index('COLUMN_NAME')['DATA_TYPE'].to_dict()

    column_datatypes_lengths = pd.read_sql(
        f" SELECT UPPER(COLUMN_NAME) as COLUMN_NAME, CONCAT_WS('|',COALESCE(CHARACTER_MAXIMUM_LENGTH,0),"
        f"COALESCE(NUMERIC_PRECISION,0),COALESCE(NUMERIC_PRECISION_RADIX,0)) AS LENGTH"
        f" FROM {db}.INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME= '{table_name}' "
        f" AND TABLE_SCHEMA ='{schema}' ", connection)
    
    column_datatypes_lengths = column_datatypes_lengths.set_index('COLUMN_NAME')['LENGTH'].to_dict()

    return column_datatypes, column_datatypes_lengths

def check_data_types_and_lengths(source_connection, target_connection, source_schema, target_schema, source_db,
                                 target_db, source_table_name, target_table_name, source_columns, target_columns,
                                 Datatype_Mapping, source_type, target_type):
    try:
        Datatype_Mapping = Datatype_Mapping.set_index('SQLServer')['Snowflake'].to_dict()

        mismatch_source_column = []
        mismatch_target_column = []
        mismatch_source_length_column = []
        mismatch_target_length_column = []

        source_column_datatypes, source_column_length = get_datatype(source_connection, source_db, source_schema,
                                                                     source_table_name)
        target_column_datatypes, target_column_length = get_datatype(target_connection, target_db, target_schema,
                                                                     target_table_name)

        if source_type == target_type == 'SQLSERVER':
            # print('both-sql')

            sql_sql = {'TINYINT': 'NUMERICS', 'SMALLINT': 'NUMERICS', 'INT': 'NUMERICS', 'BIGINT': 'NUMERICS',
                       'SMALLMONEY': 'NUMERICS', 'MONEY': 'NUMERICS', 'REAL': 'APP_NUMERICS', 'FLOAT': 'APP_NUMERICS',
                       'DECIMAL': 'NUMERICS', 'NUMERIC': 'NUMERICS', 'BIT': 'NUMERICS', 'CHAR': 'CHAR_STR',
                       'VARCHAR': 'CHAR_STR', 'TEXT': 'CHAR_STR', 'NCHAR': 'UNICODE_CHAR_STR',
                       'NVARCHAR': 'UNICODE_CHAR_STR', 'NTEXT': 'UNICODE_CHAR_STR', 'BINARY': 'BINARY_FAM',
                       'VARBINARY': 'BINARY_FAM', 'IMAGE': 'BINARY_FAM', 'DATE': 'DATE', 'TIME': 'TIME',
                       'DATETIME2': 'DATETIME2', 'DATETIMEOFFSET': 'DATETIME2', 'DATETIME': 'DATETIME',
                       'SMALLDATETIME': 'DATETIME'}

            for i in range(len(source_columns)):
                src = sql_sql[source_column_datatypes[source_columns[i]]]
                tgt = sql_sql[target_column_datatypes[target_columns[i]]]
                src_dt = source_column_length[source_columns[i]]
                tgt_dt = target_column_length[target_columns[i]]

                if (src == tgt) and (src_dt != tgt_dt):
                    s = src_dt.split('|')
                    s1, s2, s3 = int(s[0]), int(s[1]), int(s[2])
                    t = tgt_dt.split('|')
                    t1, t2, t3 = int(t[0]), int(t[1]), int(t[2])
                    if s1 > t1 or s2 > t2 or s3 > t3:
                        if source_column_datatypes[source_columns[i]] != target_column_datatypes[target_columns[i]]:
                            mismatch_source_length_column.append(source_columns[i])
                            mismatch_target_length_column.append(target_columns[i])
                            mismatch_source_column.append(source_columns[i])
                            mismatch_target_column.append(target_columns[i])
                        else:
                            mismatch_source_length_column.append(source_columns[i])
                            mismatch_target_length_column.append(target_columns[i])

                elif src != tgt:
                    mismatch_source_column.append(source_columns[i])
                    mismatch_target_column.append(target_columns[i])

        elif source_type == target_type == 'SNOWFLAKE':

            for i in range(len(source_columns)):
                src = source_column_datatypes[source_columns[i]]
                tgt = target_column_datatypes[target_columns[i]]
                src_dt = source_column_length[source_columns[i]]
                tgt_dt = target_column_length[target_columns[i]]

                if (src == tgt) and (src_dt != tgt_dt):
                    s = src_dt.split('|')
                    s1, s2, s3 = int(s[0]), int(s[1]), int(s[2])
                    t = tgt_dt.split('|')
                    t1, t2, t3 = int(t[0]), int(t[1]), int(t[2])
                    if s1 > t1 or s2 > t2 or s3 > t3:
                        mismatch_source_length_column.append(source_columns[i])
                        mismatch_target_length_column.append(target_columns[i])
                elif src != tgt:
                    mismatch_source_column.append(source_columns[i])
                    mismatch_target_column.append(target_columns[i])
                else:
                    pass

        elif source_type == 'SQLSERVER':
            for i in range(len(source_columns)):
                src = Datatype_Mapping[source_column_datatypes[source_columns[i]]]
                tgt = target_column_datatypes[target_columns[i]]
                src_dt = source_column_length[source_columns[i]]
                tgt_dt = target_column_length[target_columns[i]]

                if tgt == 'FLOAT':
                    tgt_dt = '0|53|2'

                if (src == tgt) and (src_dt != tgt_dt):
                    s = src_dt.split('|')
                    s1, s2, s3 = int(s[0]), int(s[1]), int(s[2])
                    t = tgt_dt.split('|')
                    t1, t2, t3 = int(t[0]), int(t[1]), int(t[2])
                    if s1 > t1 or s2 > t2 or s3 > t3:
                        mismatch_source_length_column.append(source_columns[i])
                        mismatch_target_length_column.append(target_columns[i])
                elif src != tgt:
                    mismatch_source_column.append(source_columns[i])
                    mismatch_target_column.append(target_columns[i])
                else:
                    pass

        else:
            
            for i in range(len(source_columns)):
                src = source_column_datatypes[source_columns[i]]
                tgt = Datatype_Mapping[target_column_datatypes[target_columns[i]]]
                src_dt = source_column_length[source_columns[i]]
                tgt_dt = target_column_length[target_columns[i]]
                if src == 'FLOAT':
                    src_dt = '0|53|2'

                if (src == tgt) and (src_dt != tgt_dt):
                    s = src_dt.split('|')
                    s1, s2, s3 = int(s[0]), int(s[1]), int(s[2])
                    t = tgt_dt.split('|')
                    t1, t2, t3 = int(t[0]), int(t[1]), int(t[2])
                    if s1 > t1 or s2 > t2 or s3 > t3:
                        mismatch_source_length_column.append(source_columns[i])
                        mismatch_target_length_column.append(target_columns[i])
                elif src != tgt:
                    mismatch_source_column.append(source_columns[i])
                    mismatch_target_column.append(target_columns[i])
                else:
                    pass

        return (mismatch_source_column, mismatch_target_column, mismatch_source_length_column,
                mismatch_target_length_column)

    except Exception as e:
        print(e, end='')

def get_table_data(con, database, schema_name, table_name, columns):
    columns = ','.join(columns)
    query = f"SELECT {columns} FROM {database}.{schema_name}.{table_name}"
    data = pd.read_sql(query, con)
    data.columns = [col.upper() for col in data.columns]
    return data

def get_column_names(con, database, schema_name, table_name):
    cursor = con.cursor()
    cursor.execute(
        f"select upper(column_name) from {database}.information_schema.columns where table_name='{table_name}' and table_schema='{schema_name}'")
    columns = [row[0] for row in cursor]
    return columns

def get_table_names(con, database, schema_name):
    cursor = con.cursor()
    cursor.execute(
        f"select upper(TABLE_NAME) from {database}.information_Schema.tables where table_schema='{schema_name}'")
    table_names = [row[0] for row in cursor]
    return table_names

class UnsupportedFileTypeError(Exception):
    pass
def get_file(file_path):
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, delimiter=',', low_memory=False, encoding='cp1252')
        df.columns = map(lambda x: str(x).upper(), df.columns)
        df_columns = df.columns
    elif file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
        df.columns = map(lambda x: str(x).upper(), df.columns)
        df_columns = df.columns
    else:
        raise UnsupportedFileTypeError('file type not supported')
    return df, df_columns
def handle_file(file_path, file_type):
    try:
        df, df_columns = get_file(file_path)
        return df, df_columns
    except FileNotFoundError:
        print(f"{file_type} file not found: {file_path}")
        if not prompt_user_for_reentry(f'{file_type} file not found.'):
            return None, None
        else:
            return handle_file(input(f'Enter {file_type} file path: '), file_type)
    except pd.errors.EmptyDataError:
        print(f"{file_type} file is empty: {file_path}")
        if not prompt_user_for_reentry(f'{file_type} file is empty.'):
            return None, None
        else:
            return handle_file(input(f'Enter {file_type} file path: '), file_type)
    except pd.errors.ParserError:
        print(f"Error parsing {file_type} file: {file_path}")
        if not prompt_user_for_reentry(f'Error parsing {file_type} file.'):
            return None, None
        else:
            return handle_file(input(f'Enter {file_type} file path: '), file_type)
    except Exception:
        print(f"An unexpected error occurred with {file_type} file: {file_path}")
        if not prompt_user_for_reentry(f'An unexpected error occurred. Supports only .csv and .xlsx file as input'):
            return None, None
        else:
            return handle_file(input(f'Enter {file_type} file path: '), file_type)
def validate_tables(connection, db, schema, table_name):
    tables_list = get_table_names(connection, db, schema)
    if table_name not in tables_list:
        return False
    return True
def process_and_split_columns(source_connection, source_db, source_schema, source_table_name, source_column_names,
                              target_connection, target_db, target_schema, target_table_name, target_column_names):
    
    if isinstance(source_column_names, float) and not isinstance(target_column_names, float):
        source_column_names = target_column_names
    elif not isinstance(source_column_names, float) and isinstance(target_column_names, float):
        target_column_names = source_column_names
    else:
        pass

    if not (isinstance(source_column_names, float) and isinstance(target_column_names, float)):
        source_column_names = [i.strip() for i in source_column_names.split(',')]
        target_column_names = [i.strip() for i in target_column_names.split(',')]

        if len(source_column_names) > len(target_column_names):
            target_column_names = target_column_names + source_column_names[len(target_column_names):]
        else:
            source_column_names = source_column_names + target_column_names[len(source_column_names):]

        source_column_names = ','.join(source_column_names)
        target_column_names = ','.join(target_column_names)

    else:
        source_column_names = get_column_names(source_connection, source_db, source_schema, source_table_name)
        source_column_names.sort()
        target_column_names = get_column_names(target_connection, target_db, target_schema, target_table_name)
        target_column_names.sort()
        source_column_names = ','.join(source_column_names)
        target_column_names = ','.join(target_column_names)

        if source_column_names != target_column_names:
            source_column_names = ''
            target_column_names = ''

    source_column_list = source_column_names.split(',')
    target_column_list = target_column_names.split(',')

    return source_column_list, target_column_list
def validate_columns(connection, db, schema, table_name, column_names, df_columns):
    if connection is not None:
        columns_excel = column_names
        columns_original = get_column_names(connection, db, schema, table_name)
    else:
        columns_excel = column_names
        columns_original = df_columns

    for col in columns_excel:
        if col not in columns_original:
            return False
    return True
def check_SNOWFLAKE_key(connection, db, table_schema, table_name):
    exec = pd.read_sql(f'''SHOW PRIMARY KEYS IN DATABASE {db}''', connection)
    exec = pd.read_sql(f'''SELECT LISTAGG("column_name", ',')
                                    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                                    WHERE "schema_name" = '{table_schema}'
                                    AND "table_name" = '{table_name}';''', connection)
    key_columns = exec.iat[0, 0].upper()
    # 0 no defined key in table
    # 1-primary key found and unique
    # 2-found key but not unique(Violating constraint)

    status = 0
    if key_columns == '':
        status = 0

    else:
        exec1 = pd.read_sql(f'''SELECT CASE WHEN
                                    count(distinct {key_columns})= count({key_columns})
                                    THEN 'Primary Key' ELSE 'NOT Primary' END FROM
                                     (select distinct * from {db}.{table_schema}.{table_name})''',
                            connection)
        if exec1.iat[0, 0] == 'Primary key':
            status = 1
        else:
            status = 2
    return status, key_columns
def check_SQLSERVER_key(connection, db, table_schema, table_name):
    exec = pd.read_sql(f'''SELECT STRING_AGG (CONVERT(NVARCHAR(max),COLUMN_NAME), ',') AS Primary_Key
                                    FROM {db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
                                    JOIN {db}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
                                    ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
                                    WHERE T.TABLE_SCHEMA = '{table_schema}'
                                    AND T.TABLE_NAME = '{table_name}' AND CONSTRAINT_TYPE = 'PRIMARY KEY';''',
                       connection)

    # 0--no defined key in table
    # 1-primary key found and unique

    if exec.iat[0, 0] is None:
        return 0, ''
    return 1, exec.iat[0, 0].upper()
def get_key(source_servername, source_connection, source_db, source_table_name, source_schema, source_column_list,
            target_column_list, src):
    s_status, src_key = getattr(sys.modules[__name__], f'check_{source_servername}_key')(source_connection, source_db,
                                                                                         source_schema,
                                                                                         source_table_name)
    if s_status != 1:
        src_key = make_key(src, source_column_list)

    if src_key == '':
        return '', ''
    key = src_key.split(',')

    tgt_key = []
    for i in key:
        j = source_column_list.index(i)
        tgt_key.append(target_column_list[j])

    return src_key, ','.join(tgt_key)
def make_key(src, src_columns):
    key = ''
    is_key = 0
    for col_combination in range(1, len(src_columns) + 1):
        for cols in itertools.combinations(src_columns, col_combination):
            if src[list(cols)].notnull().all().all() and src[list(cols)].apply(tuple, axis=1).is_unique:
                key = ','.join(cols)
                is_key = 1
                break
        if is_key == 1:
            break
    if is_key == 1:
        return key
    else:
        is_key = 0
        key = ''
        for col_combination in range(1, len(src_columns) + 1):

            for cols in itertools.combinations(src_columns, col_combination):
                notnull_column = src[list(cols)].dropna()
                if notnull_column.apply(tuple, axis=1).is_unique:
                    key = ','.join(cols)
                    is_key = 1
                    break
            if is_key == 1:
                break

    return key
def is_numeric(col):
    try:
        col.astype(float)
        return True
    except Exception as e:
        return False
def is_date(col):
    try:
        pd.to_datetime(col)
        return True
    except Exception as e:
        return False
def normalize_boolean_column(col):
    col = col.astype(str).str.lower().replace({
        '1': 'true', '0': 'false', 'true': 'true', 'false': 'false',
        'True': 'true', 'False': 'false'
    })
    col = col.replace({1: 'true', 0: 'false', True: 'true', False: 'false'})
    return col
def cleaned_data(src_df, tgt_df):

    src_distinct = src_df.drop_duplicates()
    tgt_distinct = tgt_df.drop_duplicates()

    df_cleaned1 = src_distinct.dropna(how='all')
    df_cleaned2 = tgt_distinct.dropna(how='all')

    return src_distinct, tgt_distinct, df_cleaned1, df_cleaned2
def count_and_dt_validation(mismatch_column, src_df, tgt_df):
    datatype_validation = ['']
    if len(mismatch_column) != 0:
        datatype_validation[0] = 'Datatype Mismatch in ' + ','.join(mismatch_source_column)

    count_validation = []
    count_validation.append('Pass')
    count_validation.append(f'Source_Count= {len(src_df)} , Target_Count = {len(tgt_df)}')
    if len(src_df) != len(tgt_df):
        count_validation[0] = 'Fail'

    return count_validation, datatype_validation
def validation(src_df, df_cleaned1, source_columns, src_key, tgt_df, df_cleaned2, target_columns, tgt_key, record_count):

    camel_case_columns = lambda col: ''.join([word.capitalize() for word in col.split('_')])

    src_cleaned = df_cleaned1
    tgt_cleaned = df_cleaned2
    duplicate_validation = ['']
    data_validation = ['']

    c_nr_s = len(src_df) - len(src_df.dropna(how='all'))
    c_nr_t = len(tgt_df) - len(tgt_df.dropna(how='all'))

    if c_nr_s < c_nr_t:
        data_validation.append(f'{c_nr_t - c_nr_s} extra empty records populated in target ')

    if c_nr_s > c_nr_t:
        data_validation.append(f'{c_nr_s - c_nr_t} missing empty records in target')

    src_key = src_key.split(',')
    tgt_key = tgt_key.split(',')

    boolean_like_values = {'1', '0', 'true', 'false', 'True', 'False', 1, 0, True, False}
    columns_to_bool = []
    columns_to_numeric = []
    columns_to_date = []
    tgt_null_column=[]

    for i in range(len(source_columns)):
        if source_columns[i] not in src_key:
            unique_vals_df1 = df_cleaned1[source_columns[i]].dropna().unique()
            unique_vals_df2 = df_cleaned2[target_columns[i]].dropna().unique()
            if len(unique_vals_df1)!=0 and  len(unique_vals_df2)==0:
                tgt_null_column.append(f'"{camel_case_columns(target_columns[i])}"')

            if set(unique_vals_df1).issubset(boolean_like_values) or set(unique_vals_df2).issubset(
                    boolean_like_values):

                columns_to_bool.append(i)
            else:
                if is_numeric(unique_vals_df1) and is_numeric(unique_vals_df2):
                    columns_to_numeric.append(i)
                elif is_date(unique_vals_df1) and is_date(unique_vals_df2):
                    columns_to_date.append(i)
                else:
                    pass
    if len(tgt_null_column)>0:
        data_validation.append('In Target '+','.join(tgt_null_column)+' columns populated as nulls')

    for i in range(len(source_columns)):

        col_df1 = df_cleaned1[source_columns[i]]
        col_df2 = df_cleaned2[target_columns[i]]

        if i in columns_to_bool:
            col_df1 = normalize_boolean_column(col_df1)
            col_df2 = normalize_boolean_column(col_df2)
        elif i in columns_to_numeric:
            col_df1 = col_df1.astype(float)
            col_df2 = col_df2.astype(float)
            col_df1 = col_df1.fillna(-9999)
            col_df2 = col_df2.fillna(-9999)
        elif i in columns_to_date:
            col_df1 = pd.to_datetime(col_df1)
            col_df2 = pd.to_datetime(col_df2)
        else:
            col_df1 = col_df1.astype(str)
            col_df2 = col_df2.astype(str)

        df_cleaned1[source_columns[i]] = col_df1
        df_cleaned2[target_columns[i]] = col_df2

    if src_key[0] != '':

        merge_data = df_cleaned1.merge(df_cleaned2, left_on=src_key, right_on=tgt_key, how='outer',
                                       indicator=True)

        left_in_Src = merge_data[merge_data['_merge'] == 'left_only']
        right_in_tgt = merge_data[merge_data['_merge'] == 'right_only']

        if 'left_only' in merge_data['_merge'].values:
            data_validation.append(f'got {len(left_in_Src)} extra records in source.')

        if 'right_only' in merge_data['_merge'].values:
            data_validation.append(f'got {len(right_in_tgt)} extra records in target.')

        mismatch_counts = {}

        common_records = merge_data[merge_data['_merge'] == 'both']

        primary_keys_dict = {}

        for i in range(len(source_columns)):
            if source_columns[i] not in src_key:
                if source_columns[i] == target_columns[i]:
                    col_df1 = common_records[source_columns[i] + "_x"]
                    col_df2 = common_records[target_columns[i] + "_y"]
                else:
                    col_df1 = common_records[source_columns[i]]
                    col_df2 = common_records[target_columns[i]]

                mismatches = (col_df1 != col_df2).sum()
                mismatch_counts[f'"{camel_case_columns(source_columns[i])}"'] = mismatches.sum()
                if mismatches > 0:
                    mismatch_rows = col_df1[col_df1 != col_df2].index
                    primary_keys = merge_data.loc[mismatch_rows][src_key]
                    primary_keys_dict[source_columns[i]] = primary_keys.values.tolist()

        data_dict = {}
        for column, primary_keys_list in primary_keys_dict.items():

            for primary_keys in primary_keys_list:
                squery = []
                tquery = []
                for col, value in zip(src_key, primary_keys):
                    squery.append(f"{col}=='{value}' ")
                for col, value in zip(tgt_key, primary_keys):
                    tquery.append(f"{col}=='{value}' ")
                q1 = " and ".join(squery)
                q2 = " and ".join(tquery)

                s = src_cleaned.query(q1)[[column]]
                t = tgt_cleaned.query(q2)[[column]]

                src_values = (s.values.tolist())[0]
                tgt_values = (t.values.tolist())[0]

                if column in data_dict.keys():
                    data_dict[column].append([primary_keys, src_values, tgt_values])
                else:
                    data_dict[column] = [[primary_keys, src_values, tgt_values]]

        d = list(f'{k} = {v}' for k, v in mismatch_counts.items() if v > 0)

        if len(d) > 0:
            data_validation.append('The data mismatch count column wise ' + ' , '.join(d))

        for col in data_dict:
            n = len(data_dict[col])
            pos = source_columns.index(col)
            col2 = target_columns[pos]
        
            record_count = int(record_count)

            if record_count <= n:

                for i in range(record_count):
                    s = ''
                    for (a, b) in zip(src_key, data_dict[col][i][0]):
                        s += f' "{camel_case_columns(str(a))}" = {b} ,'
                    s = s.rstrip(',')

                    data_validation.append(f'Mismatched data: Source_"{camel_case_columns(col)}"= {data_dict[col][i][1][0] if data_dict[col][i][1][0] !="none" else "null"},  Target_"{camel_case_columns(col2)}"= {data_dict[col][i][2][0] if data_dict[col][i][2][0] != "none" else "null"} for Key columns {s}')

            else:
                for i in data_dict[col]:
                    s = ''
                    for (a, b) in zip(src_key, i[0]):
                        s += f' "{camel_case_columns(str(a))}" = {b} ,'
                    s = s.rstrip(',')
                    data_validation.append(f'Mismatched Data: Source_"{camel_case_columns(col)}"= {i[1][0] if i[1][0] !="none" else "null"}, Target_"{camel_case_columns(col2)}"= {i[2][0] if i[2][0]!= "none" else "null"} for key columns {s}')

        src_duplicates = src_df[src_df.duplicated()]
        tgt_duplicates = tgt_df[tgt_df.duplicated()]

        if len(src_duplicates) == 0 and len(tgt_duplicates) == 0:
            pass
        elif len(src_duplicates) == 0:
            duplicate_validation.append(f'{len(tgt_duplicates)} Extra Duplicate records are populated into target')
        elif len(tgt_duplicates) == 0:
            duplicate_validation.append('Warning : No duplicate records are populated')
        else:
            src_duplicates = src_duplicates.dropna(how='all')
            tgt_duplicates = tgt_duplicates.dropna(how='all')

            grp = src_duplicates.groupby(src_key, dropna=False).value_counts()
            src_groupby = grp.reset_index()

            grp = tgt_duplicates.groupby(tgt_key, dropna=False).value_counts()
            tgt_groupby = grp.reset_index()

            src_groupby[src_key] = src_groupby[src_key].astype(str)
            tgt_groupby[tgt_key] = tgt_groupby[tgt_key].astype(str)

            merge_data = src_groupby.merge(tgt_groupby, left_on=src_key, right_on=tgt_key, how='outer',
                                           indicator=True)

            count_match = merge_data[merge_data['count_x'] == merge_data['count_y']]

            src_grt = merge_data[merge_data['count_x'] > merge_data['count_y']]

            tgt_grt = merge_data[merge_data['count_x'] < merge_data['count_y']]

            if len(count_match) > 0:
                duplicate_validation.append(
                    f'WARNING: {len(count_match)} Duplicate records are exactly same in both Source and Target.')
            if len(src_grt) > 0:
                duplicate_validation.append(
                    f'WARNING: {len(src_grt)} Duplicate records are partially loaded into Target.')
            if len(tgt_grt) > 0:
                duplicate_validation.append(f'{len(tgt_grt)} Duplicate records are loaded extra into Target.')

            left_in_Src = merge_data[merge_data['_merge'] == 'left_only']
            right_in_tgt = merge_data[merge_data['_merge'] == 'left_only']

            if 'left_only' in merge_data['_merge'].values:
                duplicate_validation.append(f'{len(left_in_Src)} Duplicate records did not get populated in Target.')
            if 'right_only' in merge_data['_merge'].values:
                duplicate_validation.append(f'{len(right_in_tgt)} Extra Duplicate records are populated in Target.')

        if len(data_validation) > 1:
            data_validation.pop(0)
        if len(duplicate_validation) > 1:
            duplicate_validation.pop(0)

    return data_validation, duplicate_validation
def write_output(connection, server_type, database, schema, table_name, source_table_name, target_table_name,
                 Count_validation, Datatype_validation, Data_Validation, Duplicate_Validation, mismatch_length_source):
    
    camel_case_columns = lambda col: ''.join([word.capitalize() for word in col.split('_')])
    source_table_name=camel_case_columns(source_table_name)
    target_table_name=camel_case_columns(target_table_name)

    dtv = 'Pass' if Datatype_validation[0] == '' else 'Fail'
    dv = 'Pass' if Data_Validation[0] == '' else 'Fail'
    dup = 'Pass' if Duplicate_Validation[0] == '' else 'Fail'

    data_to_write = [
        (source_table_name, target_table_name, 'Count validation', Count_validation[0], Count_validation[1]),
        (source_table_name, target_table_name, 'Datatype Validation', dtv, Datatype_validation[0])
    ]

    if len(mismatch_length_source) > 0:
        dl = 'WARNING: Truncation may occur in these columns :' + ','.join(mismatch_length_source) + ' columns'
        data_to_write.append((source_table_name, target_table_name, 'Datatype Validation', 'Fail', dl))

    for i in range(len(Data_Validation)):
        data_to_write.append((source_table_name, target_table_name, 'Data Validation', dv, Data_Validation[i]))

    for i in range(len(Duplicate_Validation)):
        data_to_write.append(
            (source_table_name, target_table_name, 'Duplicate Validation', dup, Duplicate_Validation[i]))
    # Sort the data_to_write list based on the Validation_Result ('Fail' or 'Pass')
    data_to_write.sort(key=lambda x: (x[3] != 'Fail', x[3]))

    if server_type == 'SQLSERVER':

        cursor = connection.cursor()

        cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM {database}.sys.tables t JOIN sys.schemas s ON (t.schema_id = s.schema_id) "
            f"WHERE s.name = '{schema}' AND t.name = '{table_name}') CREATE TABLE {database}.{schema}.{table_name} "
            f"(source_table varchar(100),target_table varchar(100),"
            f"Validation_type varchar(100),Validation_Result varchar(10),Reason varchar(max),"
            f"execution_time datetime default getdate());")

        for record in data_to_write:
            cursor.execute(
                f" insert into {database}.{schema}.{table_name} "
                f"(source_table, target_table, Validation_type,Validation_Result, Reason) values "
                f"('{record[0]}','{record[1]}','{record[2]}','{record[3]}','{record[4]}');")

        connection.commit()
        print('/nValidations performed and stored in desired SQL Server output location.')

    elif server_type == 'SNOWFLAKE':

        cursor = connection.cursor()

        if '\\' in source_table_name:
            source_table_name = source_table_name.replace('\\', '\\\\')
        if '\\' in target_table_name:
            target_table_name = target_table_name.replace('\\', '\\\\')

        cursor.execute(f"create TABLE  if not exists {database}.{schema}.{table_name} "
                       f"(source_table varchar,target_table varchar,Validation_type varchar, "
                       f"Validation_Result varchar,Reason varchar,"
                       f"execution_time datetime default TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP)) );")

        for record in data_to_write:
            cursor.execute(
                f" insert into {database}.{schema}.{table_name} "
                f"(source_table, target_table, Validation_type,Validation_Result, Reason) values "
                f"('{record[0]}','{record[1]}','{record[2]}','{record[3]}','{record[4]}');")

        connection.commit()
        print('/nValidations performed and stored in desired Snowflake output location.')

    else:
        file_path = 'Validation_results.csv'
        execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_exists = os.path.isfile(file_path)

        headers = ['source_table', 'target_table', 'Validation_type', 'Validation_Result', 'Reason', 'execution_time']

        with io.open(file_path, 'a', newline='') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if not file_exists:
                writer.writerow(headers)
            for row in data_to_write:
                writer.writerow(list(row) + [execution_time])
        print('/nValidations performed and stored in Validation_results.csv ')
def prompt_user_for_reentry(message):
    while True:
        response = input(message + " Do you want to re-enter the details? (yes/no): ").strip().lower()
        if response == 'yes':
            return True
        elif response == 'no':
            return False
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
def re_enter_Columns():
    source_columns = input('Enter source columns comma(,) separated.').upper()
    target_columns = input('Enter target columns comma(,) separated.').upper()
    if not source_columns:
        source_columns = math.nan
    if not target_columns:
        target_columns = math.nan
    return source_columns, target_columns

efile = r"C:\Users\Sampath.Gudisa\Desktop\Data_validation_Framework\Data_validation_input.xlsx"

#all the input details entered through UI stored in below format

global_result={
'source_type': 'SQLSERVER', 'target_type': 'SQLSERVER', 'output_type': 'SQLSERVER',
'source_server_name': 'PTDELL0032\\SQLEXPRESS', 'source_username': '', 'source_password': '',
'target_server_name': 'PTDELL0032\\SQLEXPRESS', 'target_username': '', 'target_password': '', 
'output_server_name': 'PTDELL0032\\SQLEXPRESS', 'output_username': '', 'output_password': '', 
'output_database': 'sam', 'output_schema': 'dbo',
'output_table_name': 'validation_results_new', 'output_Number of error records to be displayed': '5'
}

#assigning values to variables
source_type = global_result['source_type']

target_type = global_result['target_type']

output_type = global_result['output_type']

output_server_name = global_result.get('output_server_name')
output_account = global_result.get('output_account')
output_warehouse = global_result.get('output_warehouse')
output_username = global_result.get('output_username')
output_password = global_result.get('output_password')
output_database = global_result.get('output_database')
output_schema = global_result.get('output_schema')
output_table_name = global_result.get('output_table_name')
error_records = global_result.get('output_Number of error records to be displayed')

print()
print('Data Validation Framework \n ')

try:
    if source_type == 'SQLSERVER':
        source_server_name = global_result['source_server_name']
        source_username = global_result['source_username']
        source_password = global_result['source_password']
        source_connection = setup_SQLSERVER_connection(source_server_name, source_username, source_password)
    elif source_type == 'SNOWFLAKE':
        source_username = global_result['source_username']
        source_password = global_result['source_password']
        source_account = global_result['source_account']
        source_warehouse = global_result['source_warehouse']

        source_connection = setup_SNOWFLAKE_connection(
            source_username, source_password, source_account, source_warehouse)
    else:
        source_connection = None

except Exception as e:
    print('Failed to connect source', e)

try:
    if target_type == 'SQLSERVER':
        target_server_name = global_result['target_server_name']
        target_username = global_result['target_username']
        target_password = global_result['target_password']
        target_connection = setup_SQLSERVER_connection(target_server_name, target_username, target_password)
    elif target_type == 'SNOWFLAKE':
        target_username = global_result['target_username']
        target_password = global_result['target_password']
        target_account = global_result['target_account']
        target_warehouse = global_result['target_warehouse']
        target_connection = setup_SNOWFLAKE_connection(target_username, target_password, target_account,
                                                       target_warehouse)
    else:
        target_connection = None

except Exception as e:
    print('\n Failed to connect target', e)

output_connection = None

try:
    if output_type == 'SQLSERVER':
        output_server_name = global_result['output_server_name']
        output_username = global_result['output_username']
        output_password = global_result['output_password']
        output_connection = setup_SQLSERVER_connection(
            output_server_name, output_username, output_password)
    elif output_type == 'SNOWFLAKE':
        output_username = global_result['output_username']
        output_password = global_result['output_password']
        output_account = global_result['output_account']
        output_warehouse = global_result['output_warehouse']
        output_connection = setup_SNOWFLAKE_connection(
            output_username, output_password, output_account, output_warehouse)
    else:
        print('output connection is file\n')

except Exception as e:
    print('\n Failed to connect output source', e)


Datatype_Mapping = pd.read_excel(efile, sheet_name='SQL-Snowflake Datatype Mapping')
Datatype_Mapping = Datatype_Mapping.applymap(lambda x: x.upper() if isinstance(x, str) else x)

connection_sheet = pd.read_excel(efile, sheet_name='Input')
input3_info = connection_sheet.values.tolist()

for row in input3_info:

    source_file = row[4]
    target_file = row[8]

    row = [str(ele).upper() if isinstance(ele, str) else ele for ele in row]
    source_db, source_schema, source_table_name, source_file_path, source_columns = (row[1], row[2], row[3],
                                                                                     source_file, row[9])
    target_db, target_schema, target_table_name, target_file_path, target_columns = (row[5], row[6], row[7],
                                                                                     target_file, row[10])

    while True:

        if isinstance(source_file_path, float) and isinstance(target_file_path, float):
            print("Doing operation for: ", source_table_name, ' and ', target_table_name)
            print()

            is_src_table_valid = validate_tables(source_connection, source_db, source_schema, source_table_name)
            is_tgt_table_valid = validate_tables(target_connection, target_db, target_schema, target_table_name)

            if is_src_table_valid and is_tgt_table_valid:

                source_columns, target_columns = process_and_split_columns(
                    source_connection, source_db, source_schema, source_table_name, source_columns,
                    target_connection, target_db, target_schema, target_table_name, target_columns)

                if ((source_columns != '' and target_columns != '') and validate_columns(
                        source_connection, source_db, source_schema, source_table_name, source_columns, None)
                        and validate_columns(target_connection, target_db, target_schema, target_table_name,
                                             target_columns,
                                             None)):

                    (mismatch_source_column, mismatch_target_column, mismatch_source_length_column,
                     mismatch_target_length_column) = (
                        check_data_types_and_lengths(source_connection, target_connection, source_schema, target_schema,
                                                     source_db, target_db, source_table_name, target_table_name,
                                                     source_columns, target_columns, Datatype_Mapping,
                                                     source_type, target_type))

                    src_df = get_table_data(source_connection, source_db, source_schema, source_table_name,
                                            source_columns)
                    tgt_df = get_table_data(target_connection, target_db, target_schema, target_table_name,
                                            target_columns)

                    count_validation, datatype_validation = count_and_dt_validation(mismatch_source_column, src_df,
                                                                                    tgt_df)
                    src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)

                    src_key, tgt_key = get_key(source_type, source_connection, source_db, source_table_name,
                                               source_schema, source_columns, target_columns, df_cleaned1)

                    print('Source Primary Key Identified  :  ',src_key)
                    print('\nTarget Primary Key Identified  :  ',tgt_key)
                    data_validation, duplicate_validation = validation(src_df, df_cleaned1, source_columns,
                                                                       src_key, tgt_df, df_cleaned2, target_columns,
                                                                       tgt_key, error_records)

                    write_output(output_connection, output_type, output_database, output_schema,
                                 output_table_name,
                                 source_table_name, target_table_name, count_validation, datatype_validation,
                                 data_validation, duplicate_validation,
                                 mismatch_source_length_column)
                    break

                else:

                    if not prompt_user_for_reentry('Columns are incorrect.'):
                        break
                    else:
                        source_columns, target_columns = re_enter_Columns()

            else:

                print('Below are the Provided table details : \n ')

                print(
                    f'Source_db = {source_db},Source_schema = {source_schema},Source_table_name = {source_table_name}')
                print(
                    f'target_db = {target_db},target_schema = {target_schema},target_table_name = {target_table_name}\n')

                if not is_tgt_table_valid and not is_tgt_table_valid:
                    if not prompt_user_for_reentry('Provided src table and tgt table details are incorrect'):
                        break
                    else:
                        source_db = input('Enter source database.').upper()
                        source_schema = input('Enter source schema.').upper()
                        source_table_name = input('Enter source table name.').upper()
                        target_db = input('Enter target database.').upper()
                        target_schema = input('Enter target schema.').upper()
                        target_table_name = input('Enter target table name.').upper()

                elif not is_src_table_valid:
                    if not prompt_user_for_reentry('Provided src table details are incorrect.'):
                        break
                    else:
                        source_db = input('Enter source database.').upper()
                        source_schema = input('Enter source schema.').upper()
                        source_table_name = input('Enter source table name.').upper()

                else:
                    if not prompt_user_for_reentry('Provided tgt table details are incorrect.'):
                        break
                    else:
                        target_db = input('Enter target database.').upper()
                        target_schema = input('Enter target schema.').upper()
                        target_table_name = input('Enter target table name.').upper()

        elif (not isinstance(source_file_path, float)) and isinstance(target_file_path, float):
            print("Doing operation for: ", source_file, ' and ', target_table_name)

            is_tgt_table_valid = validate_tables(target_connection, target_db, target_schema, target_table_name)
            if is_tgt_table_valid:

                src_df, src_df_columns = handle_file(source_file, "Source")
                if src_df is None:
                    break
                if isinstance(source_columns, float) and isinstance(target_columns, float):
                    source_columns = ','.join(src_df_columns).upper()

                source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
                                                                           target_connection, target_db, target_schema,
                                                                           target_table_name, target_columns)

                if (validate_columns(None, None, None, None, source_columns, src_df_columns) and validate_columns(
                        target_connection, target_db, target_schema, target_table_name, target_columns, None)):

                    tgt_df = get_table_data(target_connection, target_db, target_schema, target_table_name,
                                            target_columns)
                    source_table_name = source_file
                    mismatch_source_column = ''
                    mismatch_source_length_column = ''

                    count_validation, datatype_validation = count_and_dt_validation(
                        mismatch_source_column, src_df, tgt_df)
                    src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)

                    src_key = make_key(df_cleaned1, source_columns)
                    src_key = src_key.split(',')
                    tgt_key = []

                    for i in src_key:
                        j = source_columns.index(i)
                        tgt_key.append(target_columns[j])
                    tgt_key = ','.join(tgt_key)
                    src_key = ','.join(src_key)

                    print('Source Primary Key Identified  :  ',src_key)
                    print('\nTarget Primary Key Identified  :  ',tgt_key)

                    data_validation, duplicate_validation = validation(src_df, df_cleaned1, source_columns,
                                                                       src_key, tgt_df, df_cleaned2, target_columns,
                                                                       tgt_key, error_records)
                    write_output(output_connection, output_type, output_database, output_schema,
                                 output_table_name, source_table_name, target_table_name, count_validation,
                                 datatype_validation, data_validation,
                                 duplicate_validation, mismatch_source_length_column)
                    break
                else:
                    if not prompt_user_for_reentry('Columns are incorrect.'):
                        break
                    else:
                        source_columns, target_columns = re_enter_Columns()
            else:
                if not prompt_user_for_reentry('Provided tgt table details are incorrect.'):
                    break
                else:
                    target_db = input('Enter target database.').upper()
                    target_schema = input('Enter target schema.').upper()
                    target_table_name = input('Enter target table name.').upper()

        elif isinstance(source_file_path, float) and (not isinstance(target_file_path, float)):
            print("Doing operation for: ", source_table_name, ' and ', target_file)

            if validate_tables(source_connection, source_db, source_schema, source_table_name):
                tgt_df, tgt_df_columns = handle_file(target_file, "Target")
                if tgt_df is None:
                    break

                if isinstance(source_columns, float) and isinstance(target_columns, float):
                    target_columns = ','.join(tgt_df_columns).upper()

                source_columns, target_columns = (process_and_split_columns(
                    source_connection, source_db, source_schema, source_table_name, source_columns,
                    None, None, None, None, target_columns))

                if (validate_columns(source_connection, source_db, source_schema, source_table_name, source_columns,
                                     None) and validate_columns(None, None, None, None,
                                                                source_columns, tgt_df_columns)):

                    src_df = get_table_data(source_connection, source_db, source_schema,
                                            source_table_name, source_columns)

                    mismatch_source_column = ''
                    mismatch_source_length_column = ''
                    target_table_name = target_file

                    count_validation, datatype_validation = count_and_dt_validation(mismatch_source_column,
                                                                                    src_df, tgt_df)
                    src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)
       
                    src_key, tgt_key = get_key(source_type, source_connection, source_db, source_table_name,
                                               source_schema, source_columns, target_columns, df_cleaned1)

                    print('Source Primary Key Identified  :  ',src_key)
                    print('\nTarget Primary Key Identified  :  ',tgt_key)

                    data_validation, duplicate_validation = validation(
                        src_df, df_cleaned1, source_columns, src_key, tgt_df,
                        df_cleaned2, target_columns, tgt_key, error_records)
                 
                    write_output(output_connection, output_type, output_database, output_schema,
                                 output_table_name, source_table_name, target_table_name, count_validation,
                                 datatype_validation, data_validation,
                                 duplicate_validation, mismatch_source_length_column)
                    break
                else:
                    if not prompt_user_for_reentry('Columns are incorrect.'):
                        break
                    else:
                        source_columns, target_columns = re_enter_Columns()

            else:
                if not prompt_user_for_reentry('Provided src table details are incorrect.'):
                    break
                else:
                    source_db = input('Enter src database.').upper()
                    source_schema = input('Enter src schema.').upper()
                    source_table_name = input('Enter src table name.').upper()

        else:
            print("Doing operation for: ", source_file, ' and ', target_file)
            print()
            src_df, src_df_columns = handle_file(source_file, "Source")
            if src_df is None:
                break
            tgt_df, tgt_df_columns = handle_file(target_file, "Target")
            if tgt_df is None:
                break

            if isinstance(source_columns, float) and isinstance(target_columns, float):
                source_columns = ','.join(sorted(src_df_columns)).upper()
                target_columns = ','.join(sorted(tgt_df_columns)).upper()
                if source_columns != target_columns:
                    source_columns = ''
                    target_columns = ''

            source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
                                                                       None, None, None, None, target_columns)

            if (source_columns != '' and target_columns != '') and (
                    validate_columns(None, None, None, None, source_columns, src_df_columns)
                    and validate_columns(None, None, None, None, target_columns, tgt_df_columns)):

                mismatch_source_column = ''
                mismatch_source_length_column = ''
                source_table_name = source_file
                target_table_name = target_file

                count_validation, datatype_validation = (
                    count_and_dt_validation(mismatch_source_column, src_df, tgt_df))
                
                src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)
                src_key = make_key(df_cleaned1, source_columns)
                tgt_key = []
                src_key = src_key.split(',')
                for i in src_key:
                    j = source_columns.index(i)
                    tgt_key.append(target_columns[j])
                tgt_key = ','.join(tgt_key)
                src_key = ','.join(src_key)
                
                print('Source Primary Key Identified  :  ',src_key)
                print('\nTarget Primary Key Identified  :  ',tgt_key)

                data_validation, duplicate_validation = (
                    validation(src_df, df_cleaned1, source_columns, src_key, tgt_df, df_cleaned2,
                               target_columns, tgt_key, error_records))
                
                write_output(output_connection, output_type, output_database, output_schema,
                             output_table_name, source_table_name, target_table_name, count_validation,
                             datatype_validation, data_validation,
                             duplicate_validation, mismatch_source_length_column)

                break

            else:

                if not prompt_user_for_reentry('Columns are incorrect.'):
                    break
                else:
                    source_columns, target_columns = re_enter_Columns()
