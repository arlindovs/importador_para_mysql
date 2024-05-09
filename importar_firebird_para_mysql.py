import fdb
import mysql.connector

import tkinter as tk
from tkinter import messagebox, filedialog

from datetime import datetime
from mysql.connector import errors

def validate_date(date_str):
    min_date = datetime.strptime('1000-01-01', '%Y-%m-%d')
    max_date = datetime.strptime('9999-12-31', '%Y-%m-%d')
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        if date < min_date or date > max_date:
            return None  # ou retorne um valor padrão
        return date_str
    except ValueError:
        return None  # ou retorne um valor padrão


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
        table_name = table[0]
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
    # Mapeamento de tipos de campo do Firebird para o MySQL
    type_mapping = {
        7: 'SMALLINT',  # Integer
        8: 'INTEGER',  # Integer
        10: 'FLOAT',  # Float
        12: 'DATE',  # Date
        13: 'TIME',  # Time
        14: 'CHAR',  # Char
        16: 'BIGINT',  # Bigint
        27: 'DOUBLE',  # Double precision
        35: 'TIMESTAMP(6)',  # Timestamp
        37: 'VARCHAR',  # Varchar
        261: 'BLOB',  # Blob sub_type text
        # Adicione mais mapeamentos conforme necessário
    }
    mysql_type = type_mapping.get(field_type, 'VARCHAR')
    if mysql_type in ['CHAR', 'VARCHAR'] and field_length is not None:
        if int(field_length) > 255:  # Altere este limite conforme necessário
            return 'TEXT'
        return f"{mysql_type}({field_length})"
    return mysql_type

def create_mysql_tables(mysql_conn, table_structure):
    mysql_cursor = mysql_conn.cursor()
    for table_name, columns in table_structure.items():
        table_name = table_name.strip()  # Remova espaços extras
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
        for column in columns:
            column_name = column[0].strip()  # Remova espaços extras
            field_length = column[3] if len(column) > 3 else None  # Verifique se column tem pelo menos 4 elementos
            column_type = convert_firebird_field_type_to_mysql(column[1], field_length)  # Passa o comprimento do campo
            is_nullable = 'NOT NULL' if column[2] is not None else 'NULL'
            create_table_query += f"`{column_name}` {column_type} {is_nullable}, "
        create_table_query = create_table_query.rstrip(', ') + ");"
        mysql_cursor.execute(create_table_query)
    mysql_cursor.close()
    

def import_data_from_firebird_to_mysql(firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port):
    # Criar um arquivo de log para salvar os registros
    log_file = "import_log.txt"
    with open(log_file, "w") as log:
        # Conectar ao banco de dados Firebird
        firebird_conn = fdb.connect(dsn=firebird_db_file, user=firebird_user, password=firebird_password, charset='latin1')

        # Conectar ao banco de dados MySQL
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            port=mysql_port
        )

        # Obter a estrutura das tabelas do Firebird
        table_structure = get_firebird_table_structure(firebird_conn)

        # Criar tabelas no MySQL
        create_mysql_tables(mysql_conn, table_structure)

        # Criar cursores para ambos os bancos de dados
        firebird_cursor = firebird_conn.cursor()
        mysql_cursor = mysql_conn.cursor()


        # Iterar sobre cada tabela do Firebird
        for table_name, columns in table_structure.items():
            # Remover espaços em branco no início e no final do nome da tabela
            table_name = table_name.strip()
            
            # Selecionar os dados da tabela do Firebird
            firebird_cursor.execute(f"SELECT * FROM {table_name}")
            rows = firebird_cursor.fetchall()
            # print(f"Dados da tabela {table_name}: {rows}")

            # Iterar sobre cada linha e inserir no MySQL
            for row in rows:
                # Construir a instrução SQL de inserção
                insert_query = f"INSERT INTO `{table_name}` ("
                for column in columns:
                    column_name = column[0].strip()  # Remover espaços em branco extras
                    insert_query += f"`{column_name}`, "
                insert_query = insert_query.rstrip(', ') + ") VALUES ("
                values = []
                for value in row:
                    if isinstance(value, str):
                        # Escapar caracteres especiais para evitar erros de sintaxe no MySQL
                        insert_query += "%s, "
                        values.append(value)
                    elif isinstance(value, datetime):  # Verificar se é uma data/hora
                        # Converter a data/hora do Firebird para o formato do MySQL
                        if value.year == 1899 and value.month == 12 and value.day == 30:
                            # Se for '30/12/1899', considerar como NULL para o MySQL
                            insert_query += "%s, "
                            values.append(None)
                        else:
                            value_str = value.strftime('%Y-%m-%d %H:%M:%S')
                            insert_query += "%s, "
                            values.append(value_str)
                    else:
                        insert_query += "%s, "
                        values.append(value)
                insert_query = insert_query.rstrip(', ') + ")"
                
                # Verificar se o número de placeholders na consulta corresponde ao número de valores
                if insert_query.count("%s") != len(values):
                    print(f"Erro: o número de placeholders na consulta não corresponde ao número de valores. Consulta: {insert_query}, Valores: {values}")
                    continue

                # Executar a instrução de inserção no MySQL
                try:
                    mysql_cursor.execute(insert_query, values)
                    print(f"Os dados foram inseridos com sucesso na tabela '{table_name}'")
                except errors.ProgrammingError as e:
                    print(f"Erro ao inserir dados na tabela '{table_name}': {e}")


            # Confirmar as alterações no MySQL
            mysql_conn.commit()


        # Fechar cursores e conexões
        firebird_cursor.close()
        mysql_cursor.close()
        firebird_conn.close()
        mysql_conn.close()

# if __name__ == "__main__":
    # Parâmetros de conexão
    # firebird_db_file = "C://import/via_caf/nge.fdb"
    # mysql_host = "localhost"
    # mysql_user = "root"
    # mysql_password = "king2sys"
    # mysql_database = "via_caf"
    # mysql_port = 3399

def submit():
    firebird_db_file = file_entry.get()
    firebird_user = firebird_user_entry.get()
    firebird_password = firebird_password_entry.get()
    mysql_host = host_entry.get()
    mysql_user = user_entry.get()
    mysql_password = password_entry.get()
    mysql_database = database_entry.get()
    mysql_port = port_entry.get()

    # Importar dados do Firebird para MySQL
    import_data_from_firebird_to_mysql(firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port)

    messagebox.showinfo("Submitted", "Connection details submitted successfully!")

def browse_file():
    filename = filedialog.askopenfilename()
    file_entry.delete(0, tk.END)
    file_entry.insert(0, filename)

def only_numbers(char):
    return char.isdigit()

root = tk.Tk()
validate_command = root.register(only_numbers)
root.geometry("500x500")

firebird_label = tk.Label(root, text="Dados Conexão do Firebird")
firebird_label.pack()

file_label = tk.Label(root, text="Firebird DB File:")
file_label.pack()
file_entry = tk.Entry(root)
file_entry.pack()
file_button = tk.Button(root, text="Browse", command=browse_file)
file_button.pack()

firebird_user_label = tk.Label(root, text="Firebird User:")
firebird_user_label.pack()
firebird_user_entry = tk.Entry(root)
firebird_user_entry.pack()
firebird_user_entry.insert(0, "sysdba")

firebird_password_label = tk.Label(root, text="Firebird Password:")
firebird_password_label.pack()
firebird_password_entry = tk.Entry(root, show="*")
firebird_password_entry.pack()
firebird_password_entry.insert(0, "masterkey")

space_label = tk.Label(root, text="")
space_label.pack()

firebird_label = tk.Label(root, text="Dados Conexão do Mysql")
firebird_label.pack()

host_label = tk.Label(root, text="MySQL Host:")
host_label.pack()
host_entry = tk.Entry(root)
host_entry.pack()
host_entry.insert(0, "localhost")

user_label = tk.Label(root, text="MySQL User:")
user_label.pack()
user_entry = tk.Entry(root)
user_entry.pack()
user_entry.insert(0, "root")

password_label = tk.Label(root, text="MySQL Password:")
password_label.pack()
password_entry = tk.Entry(root, show="*")
password_entry.pack()

database_label = tk.Label(root, text="MySQL Database:")
database_label.pack()
database_entry = tk.Entry(root)
database_entry.pack()

port_label = tk.Label(root, text="MySQL Port:")
port_label.pack()
port_entry = tk.Entry(root, validate="key", validatecommand=(validate_command, '%S'))
port_entry.pack()
port_entry.insert(0, 3306)

submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.pack()

root.mainloop()