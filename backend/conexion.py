import pyodbc
from tkinter import Tk
from tkinter import filedialog

class SQLServerConnection:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn = None

    def connect(self):
        conn_str = f'DRIVER={{ODBC Driver 11 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'

        try:
            self.conn = pyodbc.connect(conn_str)
            print('Conexión exitosa a SQL Server')

        except pyodbc.Error as ex:
            print('Error al conectar a SQL Server:', ex)

    def execute_query(self, query):
        if self.conn is None:
            print('Error: No hay conexión establecida.')
            return

        cursor = self.conn.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows

        except pyodbc.Error as ex:
            print('Error al ejecutar la consulta:', ex)

    def close_connection(self):
        try:
            if self.conn:
                self.conn.close()
                print('Conexión cerrada exitosamente.')
            else:
                print('No hay conexión para cerrar.')

        except pyodbc.Error as ex:
            print('Error al cerrar la conexión:', ex)

    def execute_script(self):
        if self.conn is None:
            print('Error: No hay conexión establecida.')
            return

        try:
            Tk().withdraw()  # Oculta la ventana principal de tkinter
            script_file_path = filedialog.askopenfilename(title="Seleccionar archivo SQL", filetypes=[("Archivos SQL", "*.sql")])

            if not script_file_path:
                print('No se seleccionó ningún archivo.')
                return

            with open(script_file_path, 'r') as script_file:
                script = script_file.read()
            print(script)
            cursor = self.conn.cursor()
            cursor.execute(script)
            print('Script ejecutado exitosamente.')
            
            

        except Exception as ex:
            print('Error al ejecutar el script:', ex)

    def load_data_from_csv(self):
        try:
            Tk().withdraw()
            csv_file_path = filedialog.askopenfilename(title="Seleccionar archivo CSV", filetypes=[("Archivos CSV", "*.csv")])
            if not csv_file_path:
                print('No se seleccionó ningún archivo CSV para cargar datos.')
                return
            

            bulk_insert_query = f"""
                BULK INSERT TEMPORAL
                FROM '{csv_file_path}'
                WITH (
                    FIELDTERMINATOR = ';',
                    ROWTERMINATOR = '\\n',
                    FIRSTROW = 2
                );
            """

            cursor = self.conn.cursor()
            cursor.execute(bulk_insert_query)
            self.conn.commit()

            print('Datos cargados desde CSV a la tabla TEMPORAL.')

        except Exception as ex:
            print('Error al cargar datos desde CSV:', ex)