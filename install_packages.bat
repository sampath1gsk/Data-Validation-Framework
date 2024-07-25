@echo off
echo Installing required Python packages...

pip install pyodbc
pip install snowflake-connector-python
pip install pandas

echo Installation complete.
