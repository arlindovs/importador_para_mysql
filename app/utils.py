# utils.py
import fdb
import mysql.connector
import subprocess
import tkinter as tk

from datetime import datetime
from mysql.connector import errors

class ErrorPopup:
    def __init__(self, root, error_message):
        self.error_popup = tk.Toplevel(root)
        self.error_popup.title("Error")
        self.error_popup.geometry("300x100")
        self.error_label = tk.Label(self.error_popup, text=error_message, padx=10, pady=10)
        self.error_label.pack()
        self.ok_button = tk.Button(self.error_popup, text="OK", command=self.error_popup.destroy)
        self.ok_button.pack()
        # Adicione isso para garantir que a janela de erro seja exibida na frente da janela principal
        self.error_popup.lift(root)

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

def get_firebird_version():
    try:
        # Executar o comando fb_version para obter a versão do Firebird
        output = subprocess.check_output(['fb_version']).decode('utf-8')
        # A saída contém a versão do Firebird
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
    

# Chamar a função para obter a versão do Firebird
firebird_version = get_firebird_version()
print("Versão do Firebird instalada:", firebird_version)

def import_data_from_firebird_to_mysql(root, firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port):    # Criar um arquivo de log para salvar os registros
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