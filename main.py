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

input_file = r"Data_validation_input.xlsx"

#all the input details entered through UI stored in below format


print('\nData Validation Framework \n ')

# Setup connections
source_connection = connect('source')
target_connection = connect('target')
output_connection = connect('output')

source_type = global_result['source_type']
target_type = global_result['target_type']
output_type = global_result['output_type']

output_database = global_result.get('output_database')
output_schema = global_result.get('output_schema')
output_table_name = global_result.get('output_table_name')
max_error_records = global_result.get('output_Number of error records to be displayed')

Datatype_Mapping = pd.read_excel(input_file, sheet_name='SQL-Snowflake Datatype Mapping')
Datatype_Mapping = Datatype_Mapping.applymap(lambda x: x.upper() if isinstance(x, str) else x)

input_sheet = pd.read_excel(input_file, sheet_name='Input')

uppercase_columns = ['SourceDB', 'SourceSchema', 'SourceTable', 'SourceColumns',
                      'TargetDB', 'TargetSchema', 'TargetTable', 'TargetColumns']

input_sheet[uppercase_columns] = input_sheet[uppercase_columns].applymap(lambda x: x.upper() if isinstance(x, str) else x)

for _,row in input_sheet.iterrows():
    
    source_db, source_schema, source_table_name, source_file, source_columns =(row['SourceDB'], row['SourceSchema'],
                                                                                      row['SourceTable'],row['SourceFilePath'], row['SourceColumns'])
    target_db, target_schema, target_table_name, target_file, target_columns = (row['TargetDB'], row['TargetSchema'],row['TargetTable'],
                                                                                     row['TargetFilePath'], row['TargetColumns'])

    while True:

        if isinstance(source_file, float) and isinstance(target_file, float):
            print("Doing operation for: ", source_table_name, ' and ', target_table_name)

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

                    print('\nSource Primary Key Identified  :  ',src_key)
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


        elif (not isinstance(source_file, float)) and isinstance(target_file, float):

            print("Doing operation for: ", source_file, ' and ', target_table_name)

            is_tgt_table_valid = validate_tables(target_connection, target_db, target_schema, target_table_name)

            if is_tgt_table_valid:
                
                source_df, source_df_columns = handle_file(source_file, "Source")
                
                if source_df is None:
                    break

                if isinstance(source_columns, float) and isinstance(target_columns, float):
                    source_columns = ','.join(source_df_columns).upper()
                

                source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
                                                                           target_connection, target_db, target_schema,
                                                                           target_table_name, target_columns)
                
                if (validate_columns(None, None, None, None, source_columns, source_columns) and validate_columns(
                        target_connection, target_db, target_schema, target_table_name, target_columns, None)):

                    target_df = get_table_data(target_connection, target_db, target_schema, target_table_name,
                                            target_columns)
                    
                    source_table_name = source_file

                    count_validation_status=count_validation(source_df,target_df)

                    datatype_validation_status=[]
                    
                    source_processed,target_processed = process_data(source_df,target_df)
                    
                    src_key = make_key(source_processed, source_columns)

                    src_key = src_key.split(',')
                    tgt_key = []

                    for i in src_key:
                        j = source_columns.index(i)
                        tgt_key.append(target_columns[j])
                    tgt_key = ','.join(tgt_key)
                    src_key = ','.join(src_key)

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
                if not prompt_user_for_reentry('Provided tgt table details are incorrect.'):
                    break
                else:
                    target_db = input('Enter target database.').upper()
                    target_schema = input('Enter target schema.').upper()
                    target_table_name = input('Enter target table name.').upper()

        elif isinstance(source_file, float) and (not isinstance(target_file, float)):
            print("Doing operation for: ", source_table_name, ' and ', target_file)
            
            is_src_table_valid=validate_tables(source_connection, source_db, source_schema, source_table_name)
            
            if is_src_table_valid :
                
                target_df, target_df_columns = handle_file(target_file, "Target")
                
                if target_df is None:
                    break

                if isinstance(source_columns, float) and isinstance(target_columns, float):
                    target_columns = ','.join(target_df_columns).upper()

                source_columns, target_columns = (process_and_split_columns(
                    source_connection, source_db, source_schema, source_table_name, source_columns,
                    None, None, None, None, target_columns))

                if (validate_columns(source_connection, source_db, source_schema, source_table_name, source_columns,
                                     None) and validate_columns(None, None, None, None,
                                                                source_columns, target_df_columns)):

                    source_df = get_table_data(source_connection, source_db, source_schema,
                                            source_table_name, source_columns)

                    target_table_name = target_file

                    count_validation_status = count_validation(source_df,target_df) 
                
                    datatype_validation_status=[]
                    
                    source_processed,target_processed = process_data(source_df,target_df)
       
                    src_key,tgt_key= get_key(source_type, source_connection, source_db, source_table_name,
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
                if not prompt_user_for_reentry('Provided src table details are incorrect.'):
                    break
                else:
                    source_db = input('Enter src database.').upper()
                    source_schema = input('Enter src schema.').upper()
                    source_table_name = input('Enter src table name.').upper()

        else:
            
            print("Doing operation for: ", source_file, ' and ', target_file)
            print()
            source_df, source_df_columns = handle_file(source_file, "Source")
            
            if source_df is None:
                break

            target_df, target_df_columns = handle_file(target_file, "Target")
            if target_df is None:
                break

            if isinstance(source_columns, float) and isinstance(target_columns, float):
                source_columns = ','.join(sorted(source_df_columns)).upper()
                target_columns = ','.join(sorted(target_df_columns)).upper()
                if source_columns != target_columns:
                    source_columns = ''
                    target_columns = ''

            source_columns, target_columns = process_and_split_columns(None, None, None, None, source_columns,
                                                                       None, None, None, None, target_columns)

            if (source_columns != '' and target_columns != '') and (
                    validate_columns(None, None, None, None, source_columns, source_df_columns)
                    and validate_columns(None, None, None, None, target_columns, target_df_columns)):

                source_table_name = source_file
                target_table_name = target_file

                count_validation_status = count_validation(source_df,target_df) 
                
                datatype_validation_status=[]
                    
                source_processed,target_processed = process_data(source_df,target_df)

                src_key = make_key(source_processed, source_columns)

                src_key = src_key.split(',')
                tgt_key = []

                for i in src_key:
                    j = source_columns.index(i)
                    tgt_key.append(target_columns[j])
                tgt_key = ','.join(tgt_key)
                src_key = ','.join(src_key)

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
