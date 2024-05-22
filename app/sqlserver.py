import pyodbc
import mysql.connector

def get_sqlserver_table_structure(sqlserver_conn, database_name):
    sqlserver_cursor = sqlserver_conn.cursor()
    tables_query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='{database_name}'"
    sqlserver_cursor.execute(tables_query)
    tables = sqlserver_cursor.fetchall()
    
    table_structure = {}
    for table in tables:
        table_name = table[0]
        columns_query = f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        sqlserver_cursor.execute(columns_query)
        columns = sqlserver_cursor.fetchall()
        table_structure[table_name] = columns
        
    sqlserver_cursor.close()
    return table_structure

def convert_sqlserver_field_type_to_mysql(field_type, max_length):
    type_mapping = {
        'int': 'INT',
        'nvarchar': 'VARCHAR',
        'datetime': 'DATETIME',
        'float': 'FLOAT',
        'bit': 'BOOLEAN',
        'varchar': 'VARCHAR',
        'char': 'CHAR',
        'text': 'TEXT',
        # Adicione mais mapeamentos conforme necessário
    }
    
    mysql_type = type_mapping.get(field_type, 'VARCHAR')
    if mysql_type == 'VARCHAR' and max_length:
        if max_length == -1:
            return 'TEXT'
        return f"{mysql_type}({max_length})"
    
    return mysql_type

def create_mysql_tables_from_sqlserver(mysql_conn, table_structure):
    mysql_cursor = mysql_conn.cursor()
    for table_name, columns in table_structure.items():
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
        for column in columns:
            column_name, data_type, max_length = column
            column_type = convert_sqlserver_field_type_to_mysql(data_type, max_length)
            create_table_query += f"`{column_name}` {column_type}, "
        create_table_query = create_table_query.rstrip(', ') + ");"
        mysql_cursor.execute(create_table_query)
    mysql_cursor.close()

def import_structure_and_data_from_sqlserver_to_mysql(sqlserver_host, sqlserver_user, sqlserver_password, sqlserver_database, sqlserver_port, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port):
    # Conectar ao banco de dados SQL Server
    sqlserver_conn = pyodbc.connect(
        driver='{SQL Server}',
        server=sqlserver_host,
        database=sqlserver_database,
        uid=sqlserver_user,
        pwd=sqlserver_password,
        port=sqlserver_port
    )

    # Conectar ao banco de dados MySQL
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        port=mysql_port
    )

    # Obtendo estrutura das tabelas do SQL Server
    table_structure = get_sqlserver_table_structure(sqlserver_conn, sqlserver_database)
    
    # Criando tabelas no MySQL
    create_mysql_tables_from_sqlserver(mysql_conn, table_structure)
    
    # Importando dados
    sqlserver_cursor = sqlserver_conn.cursor()
    mysql_cursor = mysql_conn.cursor()
    
    for table_name in table_structure.keys():
        sqlserver_cursor.execute(f"SELECT * FROM {table_name}")
        rows = sqlserver_cursor.fetchall()
        
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))
            mysql_cursor.execute(f"INSERT INTO `{table_name}` VALUES ({placeholders})", row)
    
    mysql_conn.commit()
    
    sqlserver_cursor.close()
    mysql_cursor.close()
    sqlserver_conn.close()
    mysql_conn.close()
