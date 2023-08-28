#Importamos la librería argparse para generar un CLI
import argparse
#Importamos la librería logging para generar logs
import logging
logging.basicConfig(level=logging.INFO)

#Importamos la librería pandas para analizar los datos
import pandas as pd

#Obtenemos una referencia a nuestro logger
logger = logging.getLogger(__name__)

#Definimos la Función principal
def main(file_name):
    logger.info('IniciandoProceso de limpieza de Datos...')

    #Invocamos la función para leer los datos
    df = _read_data(file_name)

    #Invocamos a la función para llenar los datos vacíos
    df = _fill_missing_data(df)

    #Invocamos a la función para eliminar registros duplicados con base al nombre
#    df = _remove_duplicate_entries(df, 'nombre')
    #Invocamos a la función para eliminar registros con valores faltantes
    df = _drop_rows_with_missing_values(df)

    #Invocamos a la función para guardar el df un archivo csv.
    _save_data(df, file_name)
    logger.info('Proceso de limpieza de datos finalizado. el nombre')
    logger.info('Archivo guardado en: {}.'.format(file_name))

    return df


    





#Funcion para leer los datos del Data Set
def _read_data(file_name):
    logger.info('Leyendo el archivo {}.'.format(file_name))
    #Leemos el archivo csv y lo devolvemos como un Data Frame
    return pd.read_csv(file_name,encoding='latin')
    #return pd.read_csv(file_name,encoding='ISO-8859-1')


#Función para llenar los datos vacíos
def _fill_missing_data(df):
    logger.info('Llenando los datos vacíos.')
    #Llenamos los datos vacíos con la palabra 'missing'
    df.replace({pd.NA: 'no hay informacion', '': 'no hay informacion'}, inplace=True)
    return df


#Función para eliminar registros duplicados con base al nombre
#def _remove_duplicate_entries(df, column_name):
#    logger.info('Eliminando registros duplicados.')
    #Eliminamos los registros duplicados con base al nombre
#    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
#    return df

#Función para eliminar registros con valores faltantes
def _drop_rows_with_missing_values(df):
    logger.info('Eliminando registros con valores faltantes.')
    #Eliminamos los registros con valores faltantes
    df.dropna(inplace=True)
    return df


#Función para guardar el df un archivo csv.
def _save_data(df, file_name):
    clean_file_name = 'clean_{}'.format(file_name)
    logger.info('Guardando los datos en el archivo {}.'.format(clean_file_name))
    #Guardamos el df en un archivo csv
    df.to_csv(clean_file_name, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file_name', help='The path to the dirty data', type=str)
    #parseamos los argumentos
    args = parser.parse_args()
    df = main(args.file_name)
    #Imprimimos el df
    print(df)
