import csv
from datetime import datetime
import io
import os
import pandas as pd
import math
import sys
import itertools

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
def validate_tables(connection, db, schema, table_name):

    tables_list = get_table_names(connection, db, schema)
    if table_name not in tables_list:
        return False
    return True
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
def write_output(connection, server_type, database, schema, table_name, source_table_name, target_table_name,
                 Count_validation_status, Datatype_validation_status, Data_Validation_status, Duplicate_Validation_status):
    
    camel_case_columns = lambda col: ''.join([word.capitalize() for word in col.split('_')])
    source_table_name=camel_case_columns(source_table_name)
    target_table_name=camel_case_columns(target_table_name)
    if Datatype_validation_status:
        dtv = 'Pass' if Datatype_validation_status[0] == '' else 'Fail'
   
    dv = 'Pass' if Data_Validation_status[0] == '' else 'Fail'
    dup = 'Pass' if Duplicate_Validation_status[0] == '' else 'Fail'

    data_to_write = [
        (source_table_name, target_table_name, 'Count validation', Count_validation_status[0], Count_validation_status[1])
    ]

    for i in range(len(Datatype_validation_status)):
        data_to_write.append((source_table_name, target_table_name, 'Datatype Validation', dtv, Datatype_validation_status[i]))

    for i in range(len(Data_Validation_status)):
        data_to_write.append((source_table_name, target_table_name, 'Data Validation', dv, Data_Validation_status[i]))

    for i in range(len(Duplicate_Validation_status)):
        data_to_write.append(
            (source_table_name, target_table_name, 'Duplicate Validation', dup, Duplicate_Validation_status[i]))
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
        print('\nValidations performed and stored in desired SQL Server output location.')

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
        print('\nValidations performed and stored in desired Snowflake output location.')

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
        print('\nValidations performed and stored in Validation_results.csv ')
def process_data (src_df, tgt_df):

    src_distinct = src_df.drop_duplicates()
    tgt_distinct = tgt_df.drop_duplicates()
    source_processed = src_distinct.dropna(how='all')
    target_processed = tgt_distinct.dropna(how='all')

    return source_processed, target_processed
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
def process_and_split_columns(source_connection, source_db, source_schema, source_table_name, source_column_names,
                              target_connection, target_db, target_schema, target_table_name, target_column_names):
    
    # print(source_column_names)
    # print(target_column_names)
    
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
