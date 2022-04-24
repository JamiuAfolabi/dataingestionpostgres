import convdftohtml
import sendemail
import pandas as pd
from credentials import db_conn
import configparser
import convdftohtml
import sendemail
import time

config = configparser.ConfigParser()
config.read('config.ini')

config_target = 'TEST_TARGET'

target_engine,conn_target =  db_conn(conn_param=config_target)

def run(schema,target_engine):
    query_audit = f"""select * from {schema}.dw_audit_table 
            where date(latest_record_time) > date(now()) - interval '2 days'"""

    query_error = f"""select * from {schema}.error_log_table
            where date(latest_record_time) > date(now()) - interval '2 days'"""
    
    
    print('Data loaded successfully')

    df_audit = pd.read_sql(query_audit,target_engine)
    df_error = pd.read_sql(query_error,target_engine)

    html_audit = convdftohtml.convertNormal(df_audit)
    html_error = convdftohtml.convertNormal(df_error)
        
    sendemail.sendEmail(schema,html_audit,'AUDIT_TABLE')
    time.sleep(5)
    sendemail.sendEmail(schema,html_error,'ERROR_LOG_TABLE')




 
