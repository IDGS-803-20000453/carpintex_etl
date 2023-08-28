import argparse
import logging
import numpy as np
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# Cargamos la configuración básica de logging
logging.basicConfig(level=logging.INFO)
# Obtenemos una referencia al logger
logger = logging.getLogger(__name__)

# Función para conectar a la base de datos
def _connect_database():
    logger.info('Conectando a la base de datos destino')
    source_server = '(localdb)\\MSSQLLocalDB'
    source_database = 'carpintext3'
    connection_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_server};DATABASE={source_database};Trusted_Connection=yes;'
    return connection_str

# Función de carga genérica que se adapta a diferentes tablas
def _load_data(file_name, table_name, columns):
    data = pd.read_csv(file_name, encoding='latin')

    # Conectamos a la base de datos
    source_connection = pyodbc.connect(_connect_database())

    # Iteramos entre las filas del CSV y vamos cargando los artículos a la base de datos
    logger.info('Iniciando la etapa de carga para el archivo ' + file_name)
    for index, row in data.iterrows():
        placeholders = ', '.join(['?'] * len(columns))
        query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'

        values = tuple(int(row[col]) if isinstance(row[col], np.int64) else row[col] for col in columns) # Modificación aquí
        source_connection.execute(query, values)
        source_connection.commit()
    source_connection.close()

# Definimos la función principal
def main(file_name):
    # Cambia las condiciones según los nombres de tus archivos CSV
    if 'Usuario' in file_name:
        _load_data(file_name, 'TablaTotalUsuarios', ['id', 'nombre', 'numeroCompras', 'Totalprecio', 'mes', 'anio'])
    elif 'Producto' in file_name:
        _load_data(file_name, 'TablaTotalProductos', ['id', 'nombre', 'cantidadTotal', 'precioTotal', 'mes', 'anio'])
    elif 'MateriaPrima' in file_name:
        _load_data(file_name, 'TablaTotalMateriaPrima', ['id', 'nombre', 'cantidadTotal', 'costoTotal', 'mes', 'anio'])
    elif 'Pedido' in file_name:
        _load_data(file_name, 'TablaTotalPedidos', ['id', 'cantidadTotal', 'Totalprecio', 'mes', 'anio'])
    elif 'Venta' in file_name:
        _load_data(file_name, 'TablaTotalVentas', ['mes', 'anio', 'numeroVentas'])

if __name__ == '__main__':
    # Definición de un nuevo parser de argumentos
    parser = argparse.ArgumentParser()
    # Creamos el argumento filename
    parser.add_argument('filename',
                        help='El archivo que deseas cargar hacia la BD',
                        type=str)
    args = parser.parse_args()
    main(args.filename)
