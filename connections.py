import pyodbc as odbc
import snowflake.connector
from input_server_details import global_result

def setup_SQLSERVER_connection(server_name, login_id, password):

    if login_id and password:
        connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server_name};"
            f"UID={login_id};"
            f"PWD={password};"
        )
    else:
        connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server_name};"
            f"Trusted_Connection=Yes"
        )
    connection = odbc.connect(connection_string)
    return connection

def setup_SNOWFLAKE_connection(username, password, account, warehouse):
    snowflake_config = {
        "user": username,
        "password": password,
        "account": account,
        "warehouse": warehouse
    }
    connection = snowflake.connector.connect(**snowflake_config)
    return connection

def setup_connection(connection_type, server_name, username, password, account=None, warehouse=None):
    if connection_type == 'SQLSERVER':
        return setup_SQLSERVER_connection(server_name, username, password)
    elif connection_type == 'SNOWFLAKE':
        return setup_SNOWFLAKE_connection(username, password, account, warehouse)
    else:
        raise ValueError(f"Unsupported connection type: {connection_type}")

def connect(prefix):
    connection_type = global_result[f'{prefix}_type']
    server_name = global_result.get(f'{prefix}_server_name', None)
    username = global_result.get(f'{prefix}_username', '')
    password = global_result.get(f'{prefix}_password', '')
    account = global_result.get(f'{prefix}_account', None)
    warehouse = global_result.get(f'{prefix}_warehouse', None)
   
    if connection_type in ['FILE','CSV']:
        # print(f'{prefix} connection is file\n')
        return None

    try:
        return setup_connection(connection_type, server_name, username, password, account, warehouse)
    except Exception as e:
        print(f'Failed to connect {prefix}:', e)
        return None

