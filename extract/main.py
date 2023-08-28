import pyodbc
import pandas as pd
import csv
import datetime
import logging

from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función conectar a la base de datos
def _connect_database():
    logger.info('Conectando a la base de datos')
    # Parámetros de conexión a la base de datos fuente (SQL Server)
    source_server = '(localdb)\\MSSQLLocalDB'
    source_database = 'carpintex'
    connection_str = f'mssql+pyodbc://{source_server}/{source_database}?driver=ODBC+Driver+17+for+SQL+Server'
    return create_engine(connection_str).connect()

def _get_data(query, mes, anio):
    logger.info(f'Recuperando los datos de la base de datos con la consulta: {query}')
    source_connection = _connect_database()
    source_data = pd.read_sql(query, source_connection, params=(mes, anio))
    source_connection.close()
    return source_data

# Función para guardar los datos de la BD en un archivo CSV
def _save_data(datosBD, tabla):
    logger.info('Guardando los datos en un archivo csv')
    ahora = datetime.datetime.now().strftime('%Y_%m_%d')
    nombre_archivo = f'Informe_{tabla}_{ahora}.csv' # Cambio en esta línea
    encabezados_csv = datosBD.columns.tolist()

    with open(nombre_archivo, mode='w+', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(encabezados_csv)
        for dato in datosBD.itertuples(index=False):
            fila = [str(getattr(dato, prop)) for prop in encabezados_csv]
            try:
                writer.writerow(fila)
            except UnicodeEncodeError:
                print('Error al intentar escribir')
                print(fila)
                continue

if __name__ == '__main__':
    mes = input("Ingresa el valor numerico del mes: ")
    anio = input("Por favor el año: ")

    tablas_queries = {
        'Venta': 'EXECUTE dbo.TotalVentas ?, ?',
        'Pedido': 'EXECUTE dbo.TotalPedidos ?, ?',
        'Producto': 'EXECUTE dbo.TotalProductos ?, ?',
        'MateriaPrima': 'EXECUTE dbo.TotalMateriaPrima ?, ?',
        'Usuario': 'EXECUTE dbo.TotalUsuarios ?, ?'
    }

    for tabla, query in tablas_queries.items():
        datosBD = _get_data(query, mes, anio)
        _save_data(datosBD, tabla)
        print(f'Num. Filas de {tabla}:' + str(len(datosBD)))
