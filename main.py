import pandas as pd
import warnings
from input_server_details import global_result
from connections import *
from help_functions import *
from validate_count import *
from validate_datatypes import *
from validate_data import *
from validate_duplicates import *

# Suppress specific warnings
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", FutureWarning)


efile = r"Data_validation_input.xlsx"

#all the input details entered through UI stored in below format

# global_result={
# 'source_type': 'SQLSERVER', 'target_type': 'SQLSERVER', 'output_type': 'SQLSERVER',
# 'source_server_name': 'PTDELL0032\\SQLEXPRESS', 'source_username': '', 'source_password': '',
# 'target_server_name': 'PTDELL0032\\SQLEXPRESS', 'target_username': '', 'target_password': '', 
# 'output_server_name': 'PTDELL0032\\SQLEXPRESS', 'output_username': '', 'output_password': '', 
# 'output_database': 'sam', 'output_schema': 'dbo',
# 'output_table_name': 'validation_results_new', 'output_Number of error records to be displayed': '5'
# }

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
max_error_records = global_result.get('output_Number of error records to be displayed')

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
    print('\nFailed to connect target', e)

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

                    source_df = get_table_data(source_connection, source_db, source_schema, source_table_name,
                                            source_columns)
                    target_df = get_table_data(target_connection, target_db, target_schema, target_table_name,
                                            target_columns)

                    count_validation_status= count_validation(source_df, target_df)
                    
                    datatype_validation_status=datatype_validation(source_connection, target_connection, source_schema, target_schema, source_db,
                                 target_db, source_table_name, target_table_name, source_columns, target_columns,
                                 Datatype_Mapping, source_type, target_type)

                    source_processed,target_processed = process_data(source_df,target_df)
                    
                    src_key, tgt_key = get_key(source_type, source_connection, source_db, source_table_name,
                                               source_schema, source_columns, target_columns, source_processed)

                    print('Source Primary Key Identified  :  ',src_key)
                    print('\nTarget Primary Key Identified  :  ',tgt_key)

                    duplicate_validation_status=duplicate_validation(source_df,target_df,src_key,tgt_key)

                    data_validation_status = data_validation(source_processed,source_columns,
                                                                    target_processed, target_columns,
                                                                    src_key,tgt_key, max_error_records)
                    
                    write_output(output_connection, output_type, output_database, output_schema,
                                 output_table_name,
                                 source_table_name, target_table_name, count_validation_status, datatype_validation_status,
                                 data_validation_status, duplicate_validation_status)
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


        # elif (not isinstance(source_file_path, float)) and isinstance(target_file_path, float):
        #     print("Doing operation for: ", source_file, ' and ', target_table_name)

        #     is_tgt_table_valid = validate_tables(target_connection, target_db, target_schema, target_table_name)
        #     if is_tgt_table_valid:

        #         src_df, src_df_columns = handle_file(source_file, "Source")
        #         if src_df is None:
        #             break
        #         if isinstance(source_columns, float) and isinstance(target_columns, float):
        #             source_columns = ','.join(src_df_columns).upper()

        #         source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
        #                                                                    target_connection, target_db, target_schema,
        #                                                                    target_table_name, target_columns)

        #         if (validate_columns(None, None, None, None, source_columns, src_df_columns) and validate_columns(
        #                 target_connection, target_db, target_schema, target_table_name, target_columns, None)):

        #             tgt_df = get_table_data(target_connection, target_db, target_schema, target_table_name,
        #                                     target_columns)
        #             source_table_name = source_file
        #             mismatch_source_column = ''
        #             mismatch_source_length_column = ''

        #             count_validation, datatype_validation = count_and_dt_validation(
        #                 mismatch_source_column, src_df, tgt_df)
        #             src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)

        #             src_key = make_key(df_cleaned1, source_columns)
        #             src_key = src_key.split(',')
        #             tgt_key = []

        #             for i in src_key:
        #                 j = source_columns.index(i)
        #                 tgt_key.append(target_columns[j])
        #             tgt_key = ','.join(tgt_key)
        #             src_key = ','.join(src_key)

        #             print('Source Primary Key Identified  :  ',src_key)
        #             print('\nTarget Primary Key Identified  :  ',tgt_key)

        #             data_validation, duplicate_validation = validation(src_df, df_cleaned1, source_columns,
        #                                                                src_key, tgt_df, df_cleaned2, target_columns,
        #                                                                tgt_key, error_records)
        #             write_output(output_connection, output_type, output_database, output_schema,
        #                          output_table_name, source_table_name, target_table_name, count_validation,
        #                          datatype_validation, data_validation,
        #                          duplicate_validation, mismatch_source_length_column)
        #             break
        #         else:
        #             if not prompt_user_for_reentry('Columns are incorrect.'):
        #                 break
        #             else:
        #                 source_columns, target_columns = re_enter_Columns()
        #     else:
        #         if not prompt_user_for_reentry('Provided tgt table details are incorrect.'):
        #             break
        #         else:
        #             target_db = input('Enter target database.').upper()
        #             target_schema = input('Enter target schema.').upper()
        #             target_table_name = input('Enter target table name.').upper()

        # elif isinstance(source_file_path, float) and (not isinstance(target_file_path, float)):
        #     print("Doing operation for: ", source_table_name, ' and ', target_file)

        #     if validate_tables(source_connection, source_db, source_schema, source_table_name):
        #         tgt_df, tgt_df_columns = handle_file(target_file, "Target")
        #         if tgt_df is None:
        #             break

        #         if isinstance(source_columns, float) and isinstance(target_columns, float):
        #             target_columns = ','.join(tgt_df_columns).upper()

        #         source_columns, target_columns = (process_and_split_columns(
        #             source_connection, source_db, source_schema, source_table_name, source_columns,
        #             None, None, None, None, target_columns))

        #         if (validate_columns(source_connection, source_db, source_schema, source_table_name, source_columns,
        #                              None) and validate_columns(None, None, None, None,
        #                                                         source_columns, tgt_df_columns)):

        #             src_df = get_table_data(source_connection, source_db, source_schema,
        #                                     source_table_name, source_columns)

        #             mismatch_source_column = ''
        #             mismatch_source_length_column = ''
        #             target_table_name = target_file

        #             count_validation, datatype_validation = count_and_dt_validation(mismatch_source_column,
        #                                                                             src_df, tgt_df)
        #             src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)
       
        #             src_key, tgt_key = get_key(source_type, source_connection, source_db, source_table_name,
        #                                        source_schema, source_columns, target_columns, df_cleaned1)

        #             print('Source Primary Key Identified  :  ',src_key)
        #             print('\nTarget Primary Key Identified  :  ',tgt_key)

        #             data_validation, duplicate_validation = validation(
        #                 src_df, df_cleaned1, source_columns, src_key, tgt_df,
        #                 df_cleaned2, target_columns, tgt_key, error_records)
                 
        #             write_output(output_connection, output_type, output_database, output_schema,
        #                          output_table_name, source_table_name, target_table_name, count_validation,
        #                          datatype_validation, data_validation,
        #                          duplicate_validation, mismatch_source_length_column)
        #             break
        #         else:
        #             if not prompt_user_for_reentry('Columns are incorrect.'):
        #                 break
        #             else:
        #                 source_columns, target_columns = re_enter_Columns()

        #     else:
        #         if not prompt_user_for_reentry('Provided src table details are incorrect.'):
        #             break
        #         else:
        #             source_db = input('Enter src database.').upper()
        #             source_schema = input('Enter src schema.').upper()
        #             source_table_name = input('Enter src table name.').upper()

        # else:
        #     print("Doing operation for: ", source_file, ' and ', target_file)
        #     print()
        #     src_df, src_df_columns = handle_file(source_file, "Source")
        #     if src_df is None:
        #         break
        #     tgt_df, tgt_df_columns = handle_file(target_file, "Target")
        #     if tgt_df is None:
        #         break

        #     if isinstance(source_columns, float) and isinstance(target_columns, float):
        #         source_columns = ','.join(sorted(src_df_columns)).upper()
        #         target_columns = ','.join(sorted(tgt_df_columns)).upper()
        #         if source_columns != target_columns:
        #             source_columns = ''
        #             target_columns = ''

        #     source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
        #                                                                None, None, None, None, target_columns)

        #     if (source_columns != '' and target_columns != '') and (
        #             validate_columns(None, None, None, None, source_columns, src_df_columns)
        #             and validate_columns(None, None, None, None, target_columns, tgt_df_columns)):

        #         mismatch_source_column = ''
        #         mismatch_source_length_column = ''
        #         source_table_name = source_file
        #         target_table_name = target_file

        #         count_validation, datatype_validation = (
        #             count_and_dt_validation(mismatch_source_column, src_df, tgt_df))
                
        #         src_distinct, tgt_distinct, df_cleaned1, df_cleaned2 = cleaned_data(src_df, tgt_df)
        #         src_key = make_key(df_cleaned1, source_columns)
        #         tgt_key = []
        #         src_key = src_key.split(',')
        #         for i in src_key:
        #             j = source_columns.index(i)
        #             tgt_key.append(target_columns[j])
        #         tgt_key = ','.join(tgt_key)
        #         src_key = ','.join(src_key)
                
        #         print('Source Primary Key Identified  :  ',src_key)
        #         print('\nTarget Primary Key Identified  :  ',tgt_key)

        #         data_validation, duplicate_validation = (
        #             validation(src_df, df_cleaned1, source_columns, src_key, tgt_df, df_cleaned2,
        #                        target_columns, tgt_key, error_records))
                
        #         write_output(output_connection, output_type, output_database, output_schema,
        #                      output_table_name, source_table_name, target_table_name, count_validation,
        #                      datatype_validation, data_validation,
        #                      duplicate_validation, mismatch_source_length_column)

        #         break

        #     else:

        #         if not prompt_user_for_reentry('Columns are incorrect.'):
        #             break
        #         else:
        #             source_columns, target_columns = re_enter_Columns()
