import re
import json
import ast
import configparser
import pandas as pd
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects.postgresql.base import PGDialect

import traceback


config = configparser.ConfigParser()
config.read('config.ini')


def dump_json_ddl(config_param, conn = None):
    """ save json of Data Definition table """

    load_online_schema = config[config_param]['LOAD_ONLINE_SCHEMA']

    schema_list = ast.literal_eval(config[config_param]["schema"])
    
    if not bool(load_online_schema):
        print('loading ddl locally')
        return None
   
    for schema in schema_list:
        print(f'start data definition for {schema}')
        dd_table = data_definition_table(conn,schema_name = schema)   
        with open(f'DW_{schema}.json','w') as file:
            json.dump(dd_table,file)
        print(f' data definition for {schema} done')
        
        
def load_json(schema):
    try:
        with open(f'DW_{schema}.json') as json_file:
            schema_dict = json.load(json_file)
    except Exception as err:
        print(err)
    return schema_dict


def data_definition_table(engine,schema_name):
    data_ddl = {}
    
    ### create an audit table if it does not exist
    audit_schema = f"""CREATE TABLE IF NOT EXISTS {schema_name}.dw_audit_table(
                        table_name text COLLATE pg_catalog."default",
                        source_rows integer,
                        dwh_rows integer,
                        variance integer,
                        latest_record_time timestamp without time zone,
                        date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                        last_loaded_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP
                    )                    
                    """
    
    
    #### Generate data definition language
    meta = MetaData(schema = schema_name)
    meta.reflect(bind=engine)
    for table in meta.sorted_tables:
        query = str(CreateTable(table).compile(engine)).replace(str(table),
                'temp_schema.temp_table').replace(str(table).split(f'{schema_name}.')[1],'temp_table')
        q2 = re.sub(r'(?s)(DEFAULT)(.*?)(NULL)', r" ", query)
        data_ddl[str(table).split(f'{schema_name}.')[1]] = q2.split('CONSTRAINT')[0][:-4] + ')'
        
    engine.execute(audit_schema)
    
    return data_ddl



def excluded_columns(dataframe_columns,primary_key = 'id'):
    try:
        return ' , '.join([col + ' = EXCLUDED.' + col for col in dataframe_columns if col not in primary_key])
    except Exception as err:
        return None


def upsert_df(data,schema,temp_table_schema,table_name,primary_key,engine,columns):
    engine.execute(""" CREATE SCHEMA IF NOT EXISTS temp_schema """)
    engine.execute("""DROP TABLE IF EXISTS temp_schema.temp_table """)
    engine.execute(temp_table_schema)
    
    upsert_excluded_query = excluded_columns(dataframe_columns = columns,primary_key = primary_key)
    while True:
        try : 
            df = next(data)
            columns = df.columns

            df.to_sql("temp_table", engine, schema='temp_schema',index=False, if_exists="append")
            upsert_query =  f""" INSERT INTO {schema}.{table_name}
            select * from temp_schema."temp_table"
            ON CONFLICT ({primary_key})
            DO UPDATE SET  {upsert_excluded_query} """
            engine.execute(upsert_query)

        except Exception as e:
            create_error_log(engine,schema,table_name,traceback.format_exc())
            print(e)
            print('Data ingestion completed')
            break


    
def table_count(table_name,schema,engine):
    try:
        count = pd.read_sql(f"select count(*) from {schema}.{table_name}",engine).iloc[0,0]
    except Exception as err:
        create_error_log(engine,schema,table_name,traceback.format_exc())
        print(err)
        count = 0
    return count


def close_connection(conn_source,
                    conn_target,
                    target_engine,
                    source_engine):
    conn_source.close()
    conn_target.close()
    target_engine.dispose()
    source_engine.dispose()    



def create_error_log(engine,schema_name,table_name,e):
    error_log_schema = f"""CREATE TABLE IF NOT EXISTS {schema_name}.error_log_table(
                        schema_name char varying(100),
                        table_name text COLLATE pg_catalog."default",
                        error char varying(4000),
                        latest_record_time timestamp without time zone default CURRENT_TIMESTAMP
                        )                    
                        """
    engine.execute(error_log_schema)

    log_query=f"""Insert into public.error_log_table
                    values ('{schema_name}','{table_name}','{str(e)[:4000]}')
                """
    try:            
        engine.execute(log_query)
    except Exception as err:
        print(err)

