
import configparser
import ast
from datetime import datetime
from msilib import schema
import pandas as pd

from credentials import  db_conn
from helper_functions import dump_json_ddl,load_json,upsert_df,table_count,close_connection

from extract import extract_source_data

from run_mail import run

config = configparser.ConfigParser()
config.read('config.ini')

config_source = 'PRODUCTION_TARGET'
config_target = 'TEST_TARGET'

source_schema = config[config_source]["schema"]
target_schema_list = ast.literal_eval(config[config_target]["schema"])

# connection
source_engine,conn_source =  db_conn(conn_param=config_source)
target_engine,conn_target =  db_conn(conn_param=config_target)

if source_engine == None or target_engine == None:
    print('Unable to load data - Either source or target engine not connected')

else:    
    # load ddl
    dump_json_ddl(conn =  target_engine,config_param=config_target)

    # Run schema load
    for target_schema in target_schema_list:
        table_ddl = load_json(target_schema,)
        
        for table in table_ddl.keys():
            temp_table_schema = table_ddl[table]
            source_table_count =  table_count(table_name = table,schema = target_schema,engine = source_engine)
            last_record_time =  datetime.now()
            
            
            initial_audit_query = f"""
                                insert into {target_schema}.dw_audit_table
                                (table_name,source_rows,latest_record_time)
                                values ('{table}',{source_table_count},'{last_record_time}')
                                """        
            
            table_data,primary_key,column = extract_source_data(
                                                        table,
                                                        source_schema = source_schema,
                                                        target_schema = target_schema ,
                                                        source_engine  = source_engine,
                                                        target_engine = target_engine 
                                                        )
            
            conn_target.execute(initial_audit_query)
            
            upsert_df(data = table_data,
                    schema = target_schema,
                    temp_table_schema = temp_table_schema,
                    table_name = table,
                    primary_key = primary_key,
                    engine = target_engine,
                    columns=column)
            
            target_table_count =  table_count(table_name = table,schema = target_schema,engine = target_engine)
            last_loaded_time =  datetime.now()
            variance = abs(source_table_count - target_table_count)
            final_audit_query = f"""
                                update {target_schema}.dw_audit_table
                                set 
                                dwh_rows = {target_table_count},
                                variance = {variance},
                                last_loaded_time = '{last_loaded_time}'
                                where date(latest_record_time) = date(now()) 
                                and table_name = '{table}'
                                """
            conn_target.execute(final_audit_query)

        run(schema=schema,target_engine=target_engine)
        

    close_connection(conn_source=conn_source,
                    conn_target=conn_target,
                    source_engine=source_engine,
                    target_engine=target_engine)

