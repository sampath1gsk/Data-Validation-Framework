from help_functions import *
import pandas as pd
import numpy as np

def data_validation( source_df,source_columns, target_df,target_columns,src_key,tgt_key,max_error_records):

    data_validation = ['']
    
    src_cleaned = source_df
    tgt_cleaned = target_df



    empty_rows_in_source = len(source_df) - len(source_df.dropna(how='all'))
    empty_rows_in_target = len(target_df) - len(target_df.dropna(how='all'))

    if empty_rows_in_source < empty_rows_in_target:
        data_validation.append(f'{empty_rows_in_target - empty_rows_in_source} extra empty records populated in target ')

    if empty_rows_in_source > empty_rows_in_target:
        data_validation.append(f'{empty_rows_in_source - empty_rows_in_target} missing empty records in target')

    src_key = src_key.split(',')
    tgt_key = tgt_key.split(',')
    
    boolean_like_values = {'1', '0', 'true', 'false', 'True', 'False', 1, 0, True, False}
    columns_to_bool = []
    columns_to_numeric = []
    columns_to_date = []
    tgt_null_column=[]
   
    for i in range(len(source_columns)):

        if source_columns[i] not in src_key:
            unique_vals_df1 = source_df[source_columns[i]].dropna().unique()
            unique_vals_df2 = target_df[target_columns[i]].dropna().unique()

            if len(unique_vals_df1)!=0 and  len(unique_vals_df2)==0:
                tgt_null_column.append(f'"{camel_case_with_underscores(target_columns[i])}"')

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

        col_df1 = source_df[source_columns[i]]
        col_df2 = target_df[target_columns[i]]
        col_df1 = col_df1.where(col_df1.notnull(), np.nan)
        col_df2 = col_df2.where(col_df2.notnull(), np.nan)

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

        source_df[source_columns[i]] = col_df1
        target_df[target_columns[i]] = col_df2

    if src_key[0] != '':

        merge_data = source_df.merge(target_df, left_on=src_key, right_on=tgt_key, how='outer',
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
                mismatch_counts[f'"{camel_case_with_underscores(source_columns[i])}"'] = mismatches.sum()
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
        
            max_error_records = int(max_error_records)

            if max_error_records <= n:

                for i in range(max_error_records):
                    s = ''
                    for (a, b) in zip(src_key, data_dict[col][i][0]):
                        s += f' "{camel_case_with_underscores(str(a))}" = {b} ,'
                    s = s.rstrip(',')

                    data_validation.append(f'Mismatched data: Source_"{camel_case_with_underscores(col)}"= {data_dict[col][i][1][0] if data_dict[col][i][1][0] !="nan" else "null"},  Target_"{camel_case_with_underscores(col2)}"= {data_dict[col][i][2][0] if data_dict[col][i][2][0] != "nan" else "null"} for Key columns {s}')

            else:
                for i in data_dict[col]:
                    s = ''
                    for (a, b) in zip(src_key, i[0]):
                        s += f' "{camel_case_with_underscores(str(a))}" = {b} ,'
                    s = s.rstrip(',')
                    data_validation.append(f'Mismatched Data: Source_"{camel_case_with_underscores(col)}"= {i[1][0] if i[1][0] !="nan" else "null"}, Target_"{camel_case_with_underscores(col2)}"= {i[2][0] if i[2][0]!= "nan" else "null"} for key columns {s}')

       
        if len(data_validation) > 1:
            data_validation.pop(0)

    return data_validation