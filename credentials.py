import configparser
from sqlalchemy import create_engine
import sqlalchemy
import urllib.parse

config = configparser.ConfigParser()
config.read('config.ini')

def db_conn(conn_param = 'PRODUCTION_TARGET'):
    """
    
    Creates a Database connection from configuration file

    Parameters
    ----------
    conn_param : str 
                default : PRODUCTION_TARGET
                optional [PRODUCTION_SOURCE,PRODUCTION_TARGET,TEST_TARGET]
    Returns
    -------
    db engine :
    db connection :
    """
    POSTGRES_ADDRESS = config[conn_param]['POSTGRES_ADDRESS']
    POSTGRES_PORT = config[conn_param]['POSTGRES_PORT']
    POSTGRES_USERNAME = config[conn_param]['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = urllib.parse.quote_plus(config[conn_param]['POSTGRES_PASSWORD'])
    POSTGRES_DBNAME = config[conn_param]['POSTGRES_DBNAME']

    conn_str = f'postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DBNAME}'
    
    try:
        engine = create_engine(conn_str)
        conn = engine.connect()
    except (sqlalchemy.exc.DBAPIError,sqlalchemy.exc.InterfaceError) as err:
        print('database could not connect\n', err)
        engine = None
        conn = None
    finally:
        return engine,conn




    
