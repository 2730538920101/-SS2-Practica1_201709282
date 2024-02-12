import conexion

server = 'LAPTOP-IT1'
database = 'practica1'
username = 'sa'
password = 'admin1234'
sql_connection = conexion.SQLServerConnection(server, database, username, password)

def guardar_resultado_en_archivo(resultados, nombre_archivo):
    if not resultados:
        print("No hay resultados para guardar.")
        return

    try:
        encabezados = [columna[0] for columna in resultados[0].cursor_description]
        filas = [tuple(fila) for fila in resultados]

        with open(nombre_archivo, 'w') as archivo:
            # Longitud máxima de cada columna
            longitudes_columnas = [max(len(str(encabezado)), max(len(str(fila[i])) for fila in filas)) for i, encabezado in enumerate(encabezados)]

            # Línea superior de la tabla
            archivo.write('+' + '+'.join('-' * (longitud + 2) for longitud in longitudes_columnas) + '+\n')

            # Encabezados
            archivo.write('| ' + ' | '.join(f"{encabezado:<{longitud}}" for encabezado, longitud in zip(encabezados, longitudes_columnas)) + ' |\n')

            # Línea de separación
            archivo.write('+' + '+'.join('-' * (longitud + 2) for longitud in longitudes_columnas) + '+\n')

            # Filas
            for fila in filas:
                archivo.write('| ' + ' | '.join(f"{str(dato):<{longitud}}" for dato, longitud in zip(fila, longitudes_columnas)) + ' |\n')
                # Separador de filas
                archivo.write('+' + '+'.join('-' * (longitud + 2) for longitud in longitudes_columnas) + '+\n')

            # Línea inferior de la tabla
            archivo.write('+' + '+'.join('-' * (longitud + 2) for longitud in longitudes_columnas) + '+\n')

        print(f"Resultados guardados en el archivo '{nombre_archivo}'.")

    except Exception as e:
        print(f"Error al guardar resultados en el archivo: {e}")





def borrar_modelo():
    print("Opción 1- Borrar modelo")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        sql_connection.execute_script()
        sql_connection.conn.commit()
        
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()


def crear_modelo():
    print("Opción 2- Crear modelo")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        sql_connection.execute_script()
        sql_connection.conn.commit()
        
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()
    

def extraer_informacion():
    print("Opción 3- Extraer información")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        sql_connection.load_data_from_csv()
        
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def cargar_informacion():
    print("Opción 4- Cargar información")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        sql_connection.execute_script()
        sql_connection.conn.commit()
        
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta1():
    print("RESULTADO CONSULTA 1:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT
    (SELECT COUNT(*) FROM TB_YEAR) AS Count_TB_YEAR,
    (SELECT COUNT(*) FROM TB_COUNTRY) AS Count_TB_COUNTRY,
    (SELECT COUNT(*) FROM TSUNAMI) AS Count_TSUNAMI;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta1.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta2():
    print("RESULTADO CONSULTA 2:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT
    TB_YEAR.Fecha,
    COUNT(TSUNAMI.ID_Tsunami) AS Cantidad_Tsunamis
    FROM
        TB_YEAR
    LEFT JOIN
        TSUNAMI ON TB_YEAR.ID_Fecha = TSUNAMI.ID_Fecha
    GROUP BY
        TB_YEAR.Fecha
    ORDER BY
    TB_YEAR.Fecha;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta2.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta3():
    print("RESULTADO CONSULTA 3:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT
    TC.Nombre AS Pais,
    TB_YEAR.Fecha AS Año,
    COUNT(TSUNAMI.ID_Tsunami) AS Cantidad_Tsunamis
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    JOIN
        TB_YEAR ON TSUNAMI.ID_Fecha = TB_YEAR.ID_Fecha
    GROUP BY
        TC.Nombre, TB_YEAR.Fecha
    ORDER BY
    TC.Nombre, TB_YEAR.Fecha;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta3.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta4():
    print("RESULTADO CONSULTA 4:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT
    TC.Nombre AS Pais,
    AVG(CONVERT(FLOAT, TSUNAMI.Total_damage)) AS Promedio_Total_Damage
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    GROUP BY
        TC.Nombre
    ORDER BY
    Pais;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta4.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta5():
    print("RESULTADO CONSULTA 5:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT TOP 5
    TC.Nombre AS Pais,
    SUM(TSUNAMI.Total_deaths) AS Total_Muertes
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    GROUP BY
        TC.Nombre
    ORDER BY
        Total_Muertes DESC;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta5.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta6():
    print("RESULTADO CONSULTA 6:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT TOP 5
    TY.Fecha AS Año,
    SUM(TSUNAMI.Total_deaths) AS Total_Muertes
    FROM
        TSUNAMI
    JOIN
        TB_YEAR TY ON TSUNAMI.ID_Fecha = TY.ID_Fecha
    GROUP BY
        TY.Fecha
    ORDER BY
        Total_Muertes DESC;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta6.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta7():
    print("RESULTADO CONSULTA 7:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT TOP 5
    TY.Fecha AS Año,
    COUNT(TSUNAMI.ID_Tsunami) AS Total_Tsunamis
    FROM
        TSUNAMI
    JOIN
        TB_YEAR TY ON TSUNAMI.ID_Fecha = TY.ID_Fecha
    GROUP BY
        TY.Fecha
    ORDER BY
        Total_Tsunamis DESC;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta7.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta8():
    print("RESULTADO CONSULTA 8:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT TOP 5
    TC.Nombre AS Pais,
    SUM(TSUNAMI.Total_houses_destroyed) AS Total_Casas_Destruidas
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    GROUP BY
        TC.Nombre
    ORDER BY
        Total_Casas_Destruidas DESC;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta8.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta9():
    print("RESULTADO CONSULTA 9:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT TOP 5
    TC.Nombre AS Pais,
    SUM(TSUNAMI.Total_houses_damaged) AS Total_Casas_Dañadas
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    GROUP BY
        TC.Nombre
    ORDER BY
        Total_Casas_Dañadas DESC;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta9.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def consulta10():
    print("RESULTADO CONSULTA 10:")
    print("")
    try:
        # Conectar a la base de datos
        sql_connection.connect()
        # Ejemplo de ejecutar una consulta
        query_result = sql_connection.execute_query('''SELECT
    TC.Nombre AS Pais,
    AVG(TSUNAMI.Water_height) AS Promedio_Altura_Agua
    FROM
        TSUNAMI
    JOIN
        TB_COUNTRY TC ON TSUNAMI.ID_Pais = TC.ID_Pais
    GROUP BY
        TC.Nombre;''')
        if query_result:
            guardar_resultado_en_archivo(query_result, 'consulta10.txt')
    finally:
        # Cerrar la conexión en un bloque 'finally' garantiza que la conexión se cierre
        sql_connection.close_connection()

def realizar_consultas():
    print("Opción 5- Realizar consultas")
    print("")
    while True:
        mostrar_submenu()
        opcion = input("Seleccione una opción (1-11): ")
        print("")
        if opcion == '1':
            consulta1()
        elif opcion == '2':
            consulta2()
        elif opcion == '3':
            consulta3()
        elif opcion == '4':
            consulta4()
        elif opcion == '5':
            consulta5()
        elif opcion == '6':
            consulta6()
        elif opcion == '7':
            consulta7()
        elif opcion == '8':
            consulta8()
        elif opcion == '9':
            consulta9()
        elif opcion == '10':
            consulta10()
        elif opcion == '11':
            break
        else:
            print("Opción no válida. Por favor, seleccione una opción del 1 al 6.")

def mostrar_menu():
    print("Menú de opciones:")
    print("1. Borrar modelo")
    print("2. Crear modelo")
    print("3. Extraer información")
    print("4. Cargar información")
    print("5. Realizar consultas")
    print("6. Salir del sistema")

def mostrar_submenu():
    print("Menú de consultas:")
    print("1. COUNT DE LAS TABLAS PARA VALIDAR LA CARGA DE DATOS")
    print("2. CANTIDAD DE TSUNAMIS POR AÑO")
    print("3. TSUNAMIS POR PAIS Y MOSTRAR LOS AÑOS EN LOS QUE HA HABIDO UN TSUNAMI")
    print("4. PROMEDIO DE TOTAL DAMAGE POR PAIS")
    print("5. TOP 5 PAISES CON MAS MUERTES")
    print("6. TOP 5 AÑOS CON MAS MUERTES")
    print("7. TOP 5 AÑOS CON MAS TSUNAMIS")
    print("8. TOP 5 PAISES CON MAS CASAS DESTRUIDAS")
    print("9. TOP 5 PAISES CON MAS CASAS DAÑADAS")
    print("10. PROMEDIO DE LA ALTURA MAXIMA DEL AGUA POR PAIS")
    print("11. REGRESAR AL MENU PRINCIPAL")

while True:
    mostrar_menu()
    opcion = input("Seleccione una opción (1-6): ")
    print("")
    if opcion == '1':
        borrar_modelo()
    elif opcion == '2':
        crear_modelo()
    elif opcion == '3':
        extraer_informacion()
    elif opcion == '4':
        cargar_informacion()
    elif opcion == '5':
        realizar_consultas()
    elif opcion == '6':
        print("Saliendo del programa. ¡Hasta luego!")
        break
    else:
        print("Opción no válida. Por favor, seleccione una opción del 1 al 6.")


