from help_functions import *
def datatype_validation(source_connection, target_connection, source_schema, target_schema, source_db,
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
                            mismatch_source_length_column.append(camel_case_with_underscores(source_columns[i]))
                            mismatch_target_length_column.append(camel_case_with_underscores(target_columns[i]))
                            mismatch_source_column.append(camel_case_with_underscores(source_columns[i]))
                            mismatch_target_column.append(camel_case_with_underscores(target_columns[i]))
                        else:
                            mismatch_source_length_column.append(camel_case_with_underscores(source_columns[i]))
                            mismatch_target_length_column.append(camel_case_with_underscores(target_columns[i]))

                elif src != tgt:
                    mismatch_source_column.append(camel_case_with_underscores(source_columns[i]))
                    mismatch_target_column.append(camel_case_with_underscores(target_columns[i]))

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
                        mismatch_source_length_column.append(camel_case_with_underscores(source_columns[i]))
                        mismatch_target_length_column.append(camel_case_with_underscores(target_columns[i]))
                elif src != tgt:
                    mismatch_source_column.append(camel_case_with_underscores(source_columns[i]))
                    mismatch_target_column.append(camel_case_with_underscores(target_columns[i]))
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
                        mismatch_source_length_column.append(camel_case_with_underscores(source_columns[i]))
                        mismatch_target_length_column.append(camel_case_with_underscores(target_columns[i]))
                elif src != tgt:
                    mismatch_source_column.append(camel_case_with_underscores(source_columns[i]))
                    mismatch_target_column.append(camel_case_with_underscores(target_columns[i]))
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
                        mismatch_source_length_column.append(camel_case_with_underscores(source_columns[i]))
                        mismatch_target_length_column.append(camel_case_with_underscores(target_columns[i]))
                elif src != tgt:
                    mismatch_source_column.append(camel_case_with_underscores(source_columns[i]))
                    mismatch_target_column.append(camel_case_with_underscores(target_columns[i]))
                else:
                    pass

        datatype_validation = ['']

        if len(mismatch_source_column) != 0:
            datatype_validation[0] = 'Datatype Mismatch in ' + ','.join(mismatch_source_column)
        if len(mismatch_source_length_column) > 0:
            datatype_validation.append('WARNING: Truncation may occur in these columns :' + ','.join(mismatch_source_length_column) + ' columns')
        return datatype_validation

    except Exception as e:
        print(e, end='')
