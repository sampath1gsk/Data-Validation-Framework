import pyodbc as odbc
import snowflake.connector
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