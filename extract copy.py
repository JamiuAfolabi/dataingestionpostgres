from typing import final
from numpy import result_type
import pandas as pd

def last_load_audit_time(table,schema,engine = None):
    query = f"select latest_record_time from {schema}.dw_audit_table where table_name = '{table}'"
    try:
        run_query = pd.read_sql_query(query, engine)['latest_record_time'][0]
        print(run_query)
    except Exception as e:
        run_query = None
        print(e)
    finally:
        print(run_query)
        print(f'{engine} in last last_load_audit_time')
        return run_query



def extract_source_data(table,
                        source_schema ,
                        target_schema ,
                        source_engine ,
                        target_engine ):
    
    last_record_time = last_load_audit_time(table,schema = target_schema,engine = target_engine)
    
    query_table_columns = f"select * from {source_schema}.{table} limit 1 "
    columns = pd.read_sql_query(query_table_columns, source_engine).columns
    # Award wind
    source_conn = source_engine.connect().execution_options(
        stream_results=True)
    
    if last_record_time == None:
        print(f'full_load for {table} table in progress; no last load date found.')
        query_data_load = f"select * from {source_schema}.{table}"
        print(query_data_load)
        latest_records = pd.read_sql_query(query_data_load, source_conn,chunksize = 1000)
        print('finish loading')
    else:
        print(f'incremental load for {table} table in progress')
        
        try :
            query_data_load = f"select * from {source_schema}.{table} where updated > '{last_record_time}' "
            latest_records = pd.read_sql_query(query_data_load, source_engine,chunksize = 10000)
        except Exception as err:
            query_data_load = f"select * from {source_schema}.{table}"
            latest_records = pd.read_sql_query(query_data_load, source_engine,chunksize = 10000)

    primary_key = get_primary_key(table_name = table,schema = target_schema,engine = source_engine)
   
    return latest_records,primary_key,columns



def get_primary_key(schema,table_name,engine = None):
    primary_key_query = f"""
    select kcu.column_name as key_column
    from information_schema.table_constraints tco
    join information_schema.key_column_usage kcu 
         on kcu.constraint_name = tco.constraint_name
         and kcu.constraint_schema = tco.constraint_schema
         and kcu.constraint_name = tco.constraint_name
    where tco.constraint_type = 'PRIMARY KEY'
        and kcu.table_schema = '{schema}'
        and kcu.table_name = '{table_name}'
        """
    try:    
        result = pd.read_sql_query(primary_key_query, con=engine).values[0]
        result = list(result)[0]
    except Exception as err:
        print(err)
        result = None
    finally:
        return result

