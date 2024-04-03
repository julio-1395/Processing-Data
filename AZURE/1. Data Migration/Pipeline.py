import pyodbc

# Connection string for SQL Server local
sql_server_conn_str = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=YourDatabase;UID=YourUsername;PWD=YourPassword'

# Connection string for Azure SQL Database
azure_sql_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=yourserver.database.windows.net;DATABASE=YourDatabase;UID=YourUsername;PWD=YourPassword'

# List of tables to migrate
tables_to_migrate = ['Table1', 'Table2', 'Table3', ...]

# Connect to SQL Server
sql_server_conn = pyodbc.connect(sql_server_conn_str)
sql_server_cursor = sql_server_conn.cursor()

# Connect to Azure SQL Database
azure_sql_conn = pyodbc.connect(azure_sql_conn_str)
azure_sql_cursor = azure_sql_conn.cursor()

def evaluate_migration():
    for table in tables_to_migrate:
        # Query data from SQL Server
        sql_server_cursor.execute(f'SELECT * FROM {table}')
        data_sql_server = sql_server_cursor.fetchall()

        # Query data from Azure SQL Database
        azure_sql_cursor.execute(f'SELECT * FROM {table}')
        data_azure_sql = azure_sql_cursor.fetchall()

        # Compare data
        if data_sql_server == data_azure_sql:
            print(f"Data migration for table {table} successful.")
        else:
            print(f"Data migration for table {table} failed.")

try:
    for table in tables_to_migrate:
        # Execute query to select data from SQL Server table
        sql_server_cursor.execute(f'SELECT * FROM {table}')

        # Fetch data from SQL Server
        data = sql_server_cursor.fetchall()

        # Insert data into corresponding table in Azure SQL Database
        for row in data:
            azure_sql_cursor.execute(f'INSERT INTO {table} VALUES (?, ?, ...)', row)
        
        # Commit changes to Azure SQL Database
        azure_sql_conn.commit()

    print("Data migration successful.")

    # Evaluate data migration
    evaluate_migration()

except Exception as e:
    print("An error occurred:", e)

finally:
    # Close connections
    sql_server_conn.close()
    azure_sql_conn.close()
