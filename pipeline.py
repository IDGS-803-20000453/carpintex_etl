import datetime
import subprocess
import logging

#Cargamos la configuracion basica de logging
logging.basicConfig(level=logging.INFO)
#Obtenemos una referencia al logger
logger = logging.getLogger(__name__)

def main():
    _extract()
    _transform()
    _load()
    logger.info('..::Proceso del ETL Terminado::.')

def _extract():
    logger.info('.::Inicio de la proceso de extraccion::.')

    
    #Ejecutamos el proceso de extraccion en la carpeta /extract 
    subprocess.run(['python', 'main.py'], cwd='./extract')  #cwd: current working directory significa que se ejecutara en la carpeta extract
        
    #Mover el archivo .csv generado a la carpeta /transform
    subprocess.run(['move', r'extract\*.csv', r'transform'], shell=True)

def _transform():
    logger.info('.::Inicio del proceso de transformación::.')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    tablas = ['Venta', 'Pedido', 'Producto', 'MateriaPrima', 'Usuario']

    for tabla in tablas:
        dirty_data_filename = f'Informe_{tabla}_{now}.csv'
        
        #Ejecutamos el proceso de transformación en la carpeta /transform
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform')

        #Eliminando el archivo .csv sucio
        subprocess.run(['del', dirty_data_filename], shell=True, cwd='./transform')

    #Mover el archivo .csv limpio a la carpeta /load
    subprocess.run(['move', r'transform\*.csv', r'load'], shell=True)


def _load():
    logger.info('.::Inicio de la proceso de carga::.')
    now = datetime.datetime.now().strftime('%Y_%m_%d')

    tables = ['Usuario', 'Producto', 'MateriaPrima', 'Pedido', 'Venta']
    for table in tables:
        clean_data_filename = f'clean_Informe_{table}_{now}.csv'
        # Ajusta la llamada al script de carga según tu necesidad
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
        # Eliminando el archivo .csv limpio
        subprocess.run(['del', clean_data_filename], shell=True, cwd='./load')


if __name__ == '__main__':
    main()
