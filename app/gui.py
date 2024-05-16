import mysql.connector
import tkinter as tk

from datetime import datetime
from tkinter import ttk, messagebox, filedialog
from datetime import datetime

from firebird import import_data_from_firebird_to_mysql
from sqlserver import import_structure_and_data_from_sqlserver_to_mysql

class DataImporterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Importador de Dados")

        # Frame para os dados de conexão do Firebird
        firebird_frame = tk.LabelFrame(self.root, text="Origem: Dados de Conexão do Firebird", padx=10, pady=10)
        firebird_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        firebird_frame.config(bg="#f0f0f0")  # Cor de fundo

        # Firebird DB File
        file_label = tk.Label(firebird_frame, text="DB File:", bg="#f0f0f0")  # Cor de fundo
        file_label.grid(row=0, column=0, sticky="w")
        self.file_entry = tk.Entry(firebird_frame, width=40)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        file_button = tk.Button(firebird_frame, text="Browse", command=self.browse_file)
        file_button.grid(row=0, column=2, padx=5, pady=5)

        # Firebird User
        user_label = tk.Label(firebird_frame, text="Usuário:", bg="#f0f0f0")  # Cor de fundo
        user_label.grid(row=1, column=0, sticky="w")
        self.firebird_user_entry = tk.Entry(firebird_frame)
        self.firebird_user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")
        self.firebird_user_entry.insert(0, 'sysdba')

        # Firebird Password
        password_label = tk.Label(firebird_frame, text="Senha:", bg="#f0f0f0")  # Cor de fundo
        password_label.grid(row=2, column=0, sticky="w")
        self.firebird_password_entry = tk.Entry(firebird_frame, show="*")
        self.firebird_password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")
        self.firebird_password_entry.insert(0, 'masterkey')

        # Separator
        separator = ttk.Separator(self.root, orient='horizontal')
        separator.grid(row=1, column=0, columnspan=3, sticky="ew", pady=10)
        
        # Frame para os dados de conexão do SqlServer
        sqlserver_frame = tk.LabelFrame(self.root, text="Origem: Dados de Conexão do SqlServer", padx=10, pady=10)
        sqlserver_frame.grid(row=2, column=0, padx=10, pady=10, sticky="nsew")
        sqlserver_frame.config(bg="#f0f0f0")  # Cor de fundo
        
        # SqlServer DB File
        file_label = tk.Label(sqlserver_frame, text="DB File:", bg="#f0f0f0")  # Cor de fundo
        file_label.grid(row=0, column=0, sticky="w")
        self.sqlserver_file_entry = tk.Entry(sqlserver_frame, width=40)
        self.sqlserver_file_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        sqlserver_file_button = tk.Button(sqlserver_frame, text="Browse", command=self.browse_sqlserver_file)
        sqlserver_file_button.grid(row=0, column=2, padx=5, pady=5)

        # SqlServer User
        user_label = tk.Label(sqlserver_frame, text="Usuário:", bg="#f0f0f0")  # Cor de fundo
        user_label.grid(row=1, column=0, sticky="w")
        self.sqlserver_user_entry = tk.Entry(sqlserver_frame)
        self.sqlserver_user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")

        # SqlServer Password
        password_label = tk.Label(sqlserver_frame, text="Senha:", bg="#f0f0f0")  # Cor de fundo
        password_label.grid(row=2, column=0, sticky="w")
        self.sqlserver_password_entry = tk.Entry(sqlserver_frame, show="*")
        self.sqlserver_password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")

        # Separator
        separator = ttk.Separator(self.root, orient='horizontal')
        separator.grid(row=3, column=0, columnspan=3, sticky="ew", pady=10)

        # Frame para os dados de conexão do MySQL
        mysql_frame = tk.LabelFrame(self.root, text="Destino: Dados de Conexão do MySQL", padx=10, pady=10)
        mysql_frame.grid(row=4, column=0, padx=10, pady=10, sticky="nsew")
        mysql_frame.config(bg="#f0f0f0")  # Cor de fundo

        # MySQL Host
        host_label = tk.Label(mysql_frame, text="Host:", bg="#f0f0f0")  # Cor de fundo
        host_label.grid(row=0, column=0, sticky="w")
        self.host_entry = tk.Entry(mysql_frame)
        self.host_entry.grid(row=0, column=1, padx=5, pady=5, sticky="we")
        self.host_entry.insert(0, "localhost")

        # MySQL User
        user_label = tk.Label(mysql_frame, text="Usuário:", bg="#f0f0f0")  # Cor de fundo
        user_label.grid(row=1, column=0, sticky="w")
        self.user_entry = tk.Entry(mysql_frame)
        self.user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="we")
        self.user_entry.insert(0, "root")

        # MySQL Password
        password_label = tk.Label(mysql_frame, text="Senha:", bg="#f0f0f0")  # Cor de fundo
        password_label.grid(row=2, column=0, sticky="w")
        self.password_entry = tk.Entry(mysql_frame, show="*")
        self.password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="we")

        # MySQL Port
        port_label = tk.Label(mysql_frame, text="Porta:", bg="#f0f0f0")  # Cor de fundo
        port_label.grid(row=3, column=0, sticky="w")
        self.port_entry = tk.Entry(mysql_frame)
        self.port_entry.grid(row=3, column=1, padx=5, pady=5, sticky="we")
        self.port_entry.insert(0, 3306)

        # MySQL Database Combobox
        self.database_label = tk.Label(mysql_frame, text="Database:", bg="#f0f0f0")  # Cor de fundo
        self.database_label.grid(row=4, column=0, sticky="w")

        self.database_combobox = ttk.Combobox(mysql_frame, state="readonly")
        self.database_combobox.grid(row=4, column=1, padx=5, pady=5, sticky="we")

        # Label para mensagens de erro
        self.error_label = tk.Label(self.root, text="", fg="red", bg="#f0f0f0")  # Cor de fundo
        self.error_label.grid(row=5, column=0, padx=10, pady=(0, 10), sticky="ew")

        # Connect Button
        self.connect_button = tk.Button(mysql_frame, text="Conectar", command=self.connect_to_mysql, bg="#007bff", fg="white")  # Cores de fundo e texto
        self.connect_button.grid(row=4, column=2, columnspan=2, pady=(5, 5))
        
        # Adicionando botões de opção para escolher a origem da importação
        self.source_selection_label = tk.Label(self.root, text="Escolha a Origem:", bg="#f0f0f0")  # Cor de fundo
        self.source_selection_label.grid(row=6, column=0, padx=10, pady=10, sticky="ew")

        self.source_selection_var = tk.StringVar()
        self.firebird_radio = tk.Radiobutton(self.root, text="Firebird", variable=self.source_selection_var, value="Firebird")
        self.firebird_radio.grid(row=6, column=1, padx=5, pady=10, sticky="ew")
        self.sqlserver_radio = tk.Radiobutton(self.root, text="SQL Server", variable=self.source_selection_var, value="SQL Server")
        self.sqlserver_radio.grid(row=6, column=2, padx=5, pady=10, sticky="ew")

        # Submit Button
        self.submit_button = tk.Button(self.root, text="Executar", command=self.submit, bg="#28a745", fg="white")  # Cores de fundo e texto
        self.submit_button.grid(row=7, column=0, padx=10, pady=10, sticky="ew")
        
        # Progress Bar
        self.progress_bar = ttk.Progressbar(self.root, orient="horizontal", length=200, mode="determinate")
        self.progress_bar.grid(row=8, column=0, padx=10, pady=10, sticky="ew")
        self.progress_bar["value"] = 0

        # Footer
        current_year = datetime.now().year
        footer_label = tk.Label(self.root, text=f"©{current_year} Copyright - CodeCoffee", anchor="e", bg="#f0f0f0")  # Cor de fundo
        footer_label.grid(row=9, column=0, padx=10, pady=(0, 10), sticky="ew")

    def browse_file(self):
        file_path = filedialog.askopenfilename(filetypes=(("Firebird Database Files", "*.fdb"), ("All files", "*.*")))
        self.file_entry.delete(0, tk.END)
        self.file_entry.insert(0, file_path)
        
    def browse_sqlserver_file(self):
        file_path = filedialog.askopenfilename(filetypes=(("SQL Server Database Files", "*.bak"), ("All files", "*.*")))
        self.sqlserver_file_entry.delete(0, tk.END)
        self.sqlserver_file_entry.insert(0, file_path)

    def connect_to_mysql(self):
        try:
            # Conectar ao MySQL para recuperar os nomes dos bancos de dados disponíveis
            mysql_conn = mysql.connector.connect(
                host=self.host_entry.get(),
                user=self.user_entry.get(),
                password=self.password_entry.get(),
                port=int(self.port_entry.get())
            )
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in mysql_cursor.fetchall()]

            # Preencher a combobox com os nomes dos bancos de dados
            self.database_combobox["values"] = databases

            # Ativar a combobox
            self.database_label.config(state="normal")
            self.database_combobox.config(state="readonly")

            # Fechar a conexão
            mysql_cursor.close()
            mysql_conn.close()
        except mysql.connector.Error as e:
            # Lidar com erros de conexão ou consulta
            messagebox.showerror("Erro", f"Erro ao conectar ao MySQL: {e}")
            
        # Destacar os campos que precisam ser preenchidos
        self.highlight_required_fields()
        
    # def highlight_required_fields(self):
    #     required_fields = [self.host_entry, self.user_entry, self.password_entry, self.port_entry]
    #     for field in required_fields:
    #         if not field.get():
    #             field.config(bg="pink")
    #         else:
    #             field.config(bg="white")

    def submit(self):
        # Verificar se os campos obrigatórios foram preenchidos
        if self.validate_fields() and self.source_selection_var.get():
            if self.source_selection_var.get() == "Firebird":
                # Coletar os dados de entrada para Firebird
                firebird_db_file = self.file_entry.get()
                firebird_user = self.firebird_user_entry.get()
                firebird_password = self.firebird_password_entry.get()
                
                # Coletar dados de entrada do MySQL
                mysql_host = self.host_entry.get()
                mysql_user = self.user_entry.get()
                mysql_password = self.password_entry.get()
                mysql_port = self.port_entry.get()
                mysql_database = self.database_combobox.get()
                
                # Atualizar a barra de progresso
                self.progress_bar["value"] = 20
                
                # Executar a função de importação do Firebird
                import_data_from_firebird_to_mysql(self.root, firebird_db_file, firebird_user, firebird_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port)
                
                # Atualizar a barra de progresso
                self.progress_bar["value"] = 100
                
                # Exibir mensagem de confirmação
                messagebox.showinfo("Importado", "Processo de importação do Firebird concluído com sucesso!")
            elif self.source_selection_var.get() == "SQL Server":
                # Coletar os dados de entrada para SQL Server
                sqlserver_db_file = self.sqlserver_file_entry.get()
                sqlserver_user = self.sqlserver_user_entry.get()
                sqlserver_password = self.sqlserver_password_entry.get()
                
                # Coletar dados de entrada do MySQL
                mysql_host = self.host_entry.get()
                mysql_user = self.user_entry.get()
                mysql_password = self.password_entry.get()
                mysql_port = self.port_entry.get()
                mysql_database = self.database_combobox.get()
                
                # Atualizar a barra de progresso
                self.progress_bar["value"] = 20
                
                # Executar a função de importação do SQL Server
                import_structure_and_data_from_sqlserver_to_mysql(self.root, sqlserver_db_file, sqlserver_user, sqlserver_password, mysql_host, mysql_user, mysql_password, mysql_database, mysql_port)
                
                # Atualizar a barra de progresso
                self.progress_bar["value"] = 100
                
                # Exibir mensagem de confirmação
                messagebox.showinfo("Importado", "Processo de importação do SQL Server concluído com sucesso!")
        else:
            # Exibir mensagem de erro se os campos obrigatórios não estiverem preenchidos ou se a origem não foi selecionada
            messagebox.showerror("Erro", "Todos os campos são obrigatórios e uma origem deve ser selecionada.")

    def validate_fields(self):
        # Verificar se a origem foi selecionada
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
