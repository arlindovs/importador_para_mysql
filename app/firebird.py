import fdb
import mysql.connector
import subprocess
import tkinter as tk
import math
from datetime import datetime
from mysql.connector import errors
from fdb.fbcore import BlobReader

class ErrorPopup:
    def __init__(self, root, error_message):
        self.error_popup = tk.Toplevel(root)
        self.error_popup.title("Error")
        self.error_popup.geometry("300x100")
        self.error_label = tk.Label(self.error_popup, text=error_message, padx=10, pady=10)
        self.error_label.pack()
        self.ok_button = tk.Button(self.error_popup, text="OK", command=self.error_popup.destroy)
        self.ok_button.pack()
        self.error_popup.lift(root)

def validate_date(date_str):
    min_date = datetime.strptime('1000-01-01', '%Y-%m-%d')
    max_date = datetime.strptime('9999-12-31', '%Y-%m-%d')
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        if date < min_date or date > max_date:
            return None
        return date_str
    except ValueError:
        return None

def get_firebird_version():
    try:
        output = subprocess.check_output(['fb_version']).decode('utf-8')
        firebird_version = output.strip()
        return firebird_version
    except Exception as e:
        print("Erro ao obter a versão do Firebird:", e)

def get_firebird_table_structure(firebird_conn):
    firebird_cursor = firebird_conn.cursor()
    tables_query = """
        SELECT RDB$RELATION_NAME 
        FROM RDB$RELATIONS 
        WHERE RDB$SYSTEM_FLAG = 0 
        AND RDB$RELATION_TYPE = 0 
        ORDER BY RDB$RELATION_NAME
    """
    firebird_cursor.execute(tables_query)
    tables = firebird_cursor.fetchall()
    table_structure = {}
    for table in tables:
        table_name = table[0].strip()
        columns_query = f"""
            SELECT 
                r.RDB$FIELD_NAME AS column_name,
                f.RDB$FIELD_TYPE AS column_type,
                r.RDB$NULL_FLAG AS is_nullable,
                f.RDB$FIELD_LENGTH AS field_length
            FROM RDB$RELATION_FIELDS r
            JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            WHERE r.RDB$RELATION_NAME = '{table_name}'
            ORDER BY r.RDB$FIELD_POSITION
        """
        firebird_cursor.execute(columns_query)
        columns = firebird_cursor.fetchall()
        table_structure[table_name] = columns
    firebird_cursor.close()
    return table_structure

def convert_firebird_field_type_to_mysql(field_type, field_length):
    type_mapping = {
        7: 'SMALLINT',
        8: 'INTEGER',
        10: 'FLOAT',
        12: 'DATE',
        13: 'TIME',
        14: 'CHAR',
        16: 'DECIMAL(18,6)',
        27: 'DOUBLE',
        35: 'TIMESTAMP(6)',
        37: 'VARCHAR',
        261: 'LONGBLOB',
    }
    mysql_type = type_mapping.get(field_type, 'VARCHAR')
    if mysql_type in ['CHAR', 'VARCHAR'] and field_length is not None:
        if int(field_length) > 255:
            return 'TEXT'
        return f"{mysql_type}({field_length})"
    return mysql_type

def drop_mysql_tables(mysql_conn):
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    mysql_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();")
    tables = mysql_cursor.fetchall()
    for table in tables:
        table_name = table[0]
        mysql_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
    mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    mysql_cursor.close()

def create_mysql_tables(mysql_conn, table_structure):
    drop_mysql_tables(mysql_conn)
    mysql_cursor = mysql_conn.cursor()
    for table_name, columns in table_structure.items():
        table_name = table_name.strip()
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
        for column in columns:
            column_name = column[0].strip()
            field_length = column[3] if len(column) > 3 else None
            column_type = convert_firebird_field_type_to_mysql(column[1], field_length)
            is_nullable = 'NOT NULL' if column[2] is not None else 'NULL'
            create_table_query += f"`{column_name}` {column_type} {is_nullable}, "
        create_table_query = create_table_query.rstrip(', ') + ");"
        mysql_cursor.execute(create_table_query)
    mysql_cursor.close()

def import_data_from_firebird_to_mysql(root, firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port):
    log_file = "import_log.txt"
    with open(log_file, "w") as log:
        firebird_conn = fdb.connect(dsn=firebird_db_file, user=firebird_user, password=firebird_password, charset='latin1')
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            port=mysql_port
        )
        table_structure = get_firebird_table_structure(firebird_conn)
        create_mysql_tables(mysql_conn, table_structure)
        firebird_cursor = firebird_conn.cursor()
        mysql_cursor = mysql_conn.cursor()

        for table_name, columns in table_structure.items():
            table_name = table_name.strip()
            firebird_cursor.execute(f"SELECT * FROM {table_name}")
            rows = firebird_cursor.fetchall()

            for row in rows:
                insert_query = f"INSERT INTO `{table_name}` ("
                for column in columns:
                    column_name = column[0].strip()
                    insert_query += f"`{column_name}`, "
                insert_query = insert_query.rstrip(', ') + ") VALUES ("
                values = []
                for value in row:
                    if isinstance(value, str):
                        insert_query += "%s, "
                        values.append(value)
                    elif isinstance(value, datetime):
                        if value.year == 1899 and value.month == 12 and value.day == 30:
                            insert_query += "%s, "
                            values.append(None)
                        else:
                            value_str = value.strftime('%Y-%m-%d %H:%M:%S')
                            insert_query += "%s, "
                            values.append(value_str)
                    elif isinstance(value, BlobReader):
                        insert_query += "%s, "
                        values.append(value.read())
                    elif isinstance(value, float) and math.isnan(value):
                        insert_query += "%s, "
                        values.append(None)
                    else:
                        insert_query += "%s, "
                        values.append(value)
                insert_query = insert_query.rstrip(', ') + ")"
                
                if insert_query.count("%s") != len(values):
                    print(f"Erro: o número de placeholders na consulta não corresponde ao número de valores. Consulta: {insert_query}, Valores: {values}")
                    continue

                try:
                    mysql_cursor.execute(insert_query, values)
                    print(f"Os dados foram inseridos com sucesso na tabela '{table_name}'")
                except errors.DataError as e:
                    print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
                    log.write(f"Erro ao inserir dados na tabela '{table_name}': {e}\n")

            mysql_conn.commit()

        firebird_cursor.close()
        mysql_cursor.close()
        firebird_conn.close()
        mysql_conn.close()
