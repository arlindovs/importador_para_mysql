import mysql.connector
import tkinter as tk
from datetime import datetime
from tkinter import ttk, messagebox, filedialog
from firebird import import_data_from_firebird_to_mysql
from sqlserver import import_structure_and_data_from_sqlserver_to_mysql

class DataImporterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Importador de Dados")
        self.root.config(bg="#f0f0f0")

        # Frame para os dados de conexão do Firebird
        firebird_frame = tk.LabelFrame(self.root, text="Origem: Dados de Conexão do Firebird", padx=10, pady=10, bg="#f0f0f0")
        firebird_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        tk.Label(firebird_frame, text="DB File:", bg="#f0f0f0").grid(row=0, column=0, sticky="w")
        self.file_entry = tk.Entry(firebird_frame, width=40)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        tk.Button(firebird_frame, text="Browse", command=self.browse_file).grid(row=0, column=2, padx=5, pady=5)

        tk.Label(firebird_frame, text="Usuário:", bg="#f0f0f0").grid(row=1, column=0, sticky="w")
        self.firebird_user_entry = tk.Entry(firebird_frame)
        self.firebird_user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")
        self.firebird_user_entry.insert(0, 'sysdba')

        tk.Label(firebird_frame, text="Senha:", bg="#f0f0f0").grid(row=2, column=0, sticky="w")
        self.firebird_password_entry = tk.Entry(firebird_frame, show="*")
        self.firebird_password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")
        self.firebird_password_entry.insert(0, 'masterkey')

        # Separator
        ttk.Separator(self.root, orient='horizontal').grid(row=1, column=0, columnspan=3, sticky="ew", pady=10)

        # Frame para os dados de conexão do SqlServer
        sqlserver_frame = tk.LabelFrame(self.root, text="Origem: Dados de Conexão do SqlServer", padx=10, pady=10, bg="#f0f0f0")
        sqlserver_frame.grid(row=2, column=0, padx=10, pady=10, sticky="nsew")

        tk.Label(sqlserver_frame, text="Host:", bg="#f0f0f0").grid(row=0, column=0, sticky="w")
        self.sqlserver_host_entry = tk.Entry(sqlserver_frame)
        self.sqlserver_host_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        self.sqlserver_host_entry.insert(0, "localhost")

        tk.Label(sqlserver_frame, text="Usuário:", bg="#f0f0f0").grid(row=1, column=0, sticky="w")
        self.sqlserver_user_entry = tk.Entry(sqlserver_frame)
        self.sqlserver_user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")

        tk.Label(sqlserver_frame, text="Senha:", bg="#f0f0f0").grid(row=2, column=0, sticky="w")
        self.sqlserver_password_entry = tk.Entry(sqlserver_frame, show="*")
        self.sqlserver_password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")
        
        tk.Label(sqlserver_frame, text="Porta:", bg="#f0f0f0").grid(row=3, column=0, sticky="w")
        self.sqlserver_port_entry = tk.Entry(sqlserver_frame)
        self.sqlserver_port_entry.grid(row=3, column=1, padx=5, pady=5, sticky="we")
        self.sqlserver_port_entry.insert(0, 3306)

        tk.Label(sqlserver_frame, text="Database:", bg="#f0f0f0").grid(row=4, column=0, sticky="w")
        self.sqlserver_database_combobox = ttk.Combobox(sqlserver_frame, state="readonly")
        self.sqlserver_database_combobox.grid(row=4, column=1, padx=5, pady=5, sticky="we")

        # Separator
        ttk.Separator(self.root, orient='horizontal').grid(row=3, column=0, columnspan=3, sticky="ew", pady=10)

        # Frame para os dados de conexão do MySQL
        mysql_frame = tk.LabelFrame(self.root, text="Destino: Dados de Conexão do MySQL", padx=10, pady=10, bg="#f0f0f0")
        mysql_frame.grid(row=4, column=0, padx=10, pady=10, sticky="nsew")

        tk.Label(mysql_frame, text="Host:", bg="#f0f0f0").grid(row=0, column=0, sticky="w")
        self.host_entry = tk.Entry(mysql_frame)
        self.host_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        self.host_entry.insert(0, "localhost")

        tk.Label(mysql_frame, text="Usuário:", bg="#f0f0f0").grid(row=1, column=0, sticky="w")
        self.user_entry = tk.Entry(mysql_frame)
        self.user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")
        self.user_entry.insert(0, "root")

        tk.Label(mysql_frame, text="Senha:", bg="#f0f0f0").grid(row=2, column=0, sticky="w")
        self.password_entry = tk.Entry(mysql_frame, show="*")
        self.password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")

        tk.Label(mysql_frame, text="Porta:", bg="#f0f0f0").grid(row=3, column=0, sticky="w")
        self.port_entry = tk.Entry(mysql_frame)
        self.port_entry.grid(row=3, column=1, padx=5, pady=5, sticky="we")
        self.port_entry.insert(0, 3306)

        tk.Label(mysql_frame, text="Database:", bg="#f0f0f0").grid(row=4, column=0, sticky="w")
        self.database_combobox = ttk.Combobox(mysql_frame, state="readonly")
        self.database_combobox.grid(row=4, column=1, padx=5, pady=5, sticky="we")

        # Label para mensagens de erro
        self.error_label = tk.Label(self.root, text="", fg="red", bg="#f0f0f0")
        self.error_label.grid(row=5, column=0, padx=10, pady=(0, 10), sticky="ew")

        tk.Button(mysql_frame, text="Conectar", command=self.connect_to_mysql, bg="#007bff", fg="white").grid(row=4, column=2, columnspan=2, pady=(5, 5))

        # Opções de origem
        tk.Label(self.root, text="Escolha a Origem:", bg="#f0f0f0").grid(row=6, column=0, padx=10, pady=10, sticky="ew")
        self.source_selection_var = tk.StringVar()
        tk.Radiobutton(self.root, text="Firebird", variable=self.source_selection_var, value="Firebird", bg="#f0f0f0").grid(row=6, column=1, padx=5, pady=10, sticky="ew")
        tk.Radiobutton(self.root, text="SQL Server", variable=self.source_selection_var, value="SQL Server", bg="#f0f0f0").grid(row=6, column=2, padx=5, pady=10, sticky="ew")

        tk.Button(self.root, text="Executar", command=self.submit, bg="#28a745", fg="white").grid(row=7, column=0, padx=10, pady=10, sticky="ew")

        self.progress_bar = ttk.Progressbar(self.root, orient="horizontal", length=200, mode="determinate")
        self.progress_bar.grid(row=8, column=0, padx=10, pady=10, sticky="ew")
        self.progress_bar["value"] = 0

        current_year = datetime.now().year
        tk.Label(self.root, text=f"©{current_year} Copyright - CodeCoffee", anchor="e", bg="#f0f0f0").grid(row=9, column=0, padx=10, pady=(0, 10), sticky="ew")

    def browse_file(self):
        file_path = filedialog.askopenfilename(filetypes=(("Firebird Database Files", "*.fdb"), ("All files", "*.*")))
        self.file_entry.delete(0, tk.END)
        self.file_entry.insert(0, file_path)

    def connect_to_mysql(self):
        try:
            mysql_conn = mysql.connector.connect(
                host=self.host_entry.get(),
                user=self.user_entry.get(),
                password=self.password_entry.get(),
                port=int(self.port_entry.get())
            )
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in mysql_cursor.fetchall()]

            self.database_combobox["values"] = databases

            mysql_cursor.close()
            mysql_conn.close()
        except mysql.connector.Error as e:
            messagebox.showerror("Erro", f"Erro ao conectar ao MySQL: {e}")

    def submit(self):
        if self.validate_fields() and self.source_selection_var.get():
            if self.source_selection_var.get() == "Firebird":
                firebird_db_file = self.file_entry.get()
                firebird_user = self.firebird_user_entry.get()
                firebird_password = self.firebird_password_entry.get()

                mysql_host = self.host_entry.get()
                mysql_user = self.user_entry.get()
                mysql_password = self.password_entry.get()
                mysql_port = self.port_entry.get()
                mysql_database = self.database_combobox.get()

                self.progress_bar["value"] = 20
                import_data_from_firebird_to_mysql(self.root, firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port)
                self.progress_bar["value"] = 100

                messagebox.showinfo("Importado", "Processo de importação do Firebird concluído com sucesso!")

            elif self.source_selection_var.get() == "SQL Server":
                sqlserver_host = self.sqlserver_host_entry.get()
                sqlserver_port = self.sqlserver_port_entry.get()
                sqlserver_database = self.sqlserver_database_combobox.get()
                sqlserver_user = self.sqlserver_user_entry.get()
                sqlserver_password = self.sqlserver_password_entry.get()
                

                mysql_host = self.host_entry.get()
                mysql_user = self.user_entry.get()
                mysql_password = self.password_entry.get()
                mysql_port = self.port_entry.get()
                mysql_database = self.database_combobox.get()

                self.progress_bar["value"] = 20
                import_structure_and_data_from_sqlserver_to_mysql(self.root, sqlserver_host, sqlserver_database, sqlserver_user, sqlserver_password, sqlserver_port, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port)
                self.progress_bar["value"] = 100

                messagebox.showinfo("Importado", "Processo de importação do SQL Server concluído com sucesso!")
        else:
            messagebox.showerror("Erro", "Todos os campos são obrigatórios e uma origem deve ser selecionada.")

    def validate_fields(self):
        if not self.source_selection_var.get():
            return False

        required_fields = []
        if self.source_selection_var.get() == "Firebird":
            required_fields = [self.file_entry, self.firebird_user_entry, self.firebird_password_entry, self.host_entry, self.user_entry, self.password_entry, self.port_entry]
        elif self.source_selection_var.get() == "SQL Server":
            required_fields = [self.sqlserver_file_entry, self.sqlserver_user_entry, self.sqlserver_password_entry, self.host_entry, self.user_entry, self.password_entry, self.port_entry]

        for field in required_fields:
            if not field.get():
                return False
        return True

def create_gui():
    root = tk.Tk()
    app = DataImporterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    create_gui()
