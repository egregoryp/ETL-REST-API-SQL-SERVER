import sqlalchemy as db
import pandas as pd
import json
import requests
from dotenv import load_dotenv
import os 

load_dotenv()
myPWD   = os.getenv("PWD")
myDB_HOST = os.getenv("DB_HOST")
myDB    = os.getenv("DB")
myUSER  = os.getenv("USER")
myPORT  = os.getenv("PORT")

'''
Create a mapping of df dtypes to mysql data types (not perfect, but close enough)
'''
def dtype_mapping():
    return {'object' : 'TEXT',
        'int64' : 'INT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'category' : 'TEXT',
        'timedelta[ns]' : 'TEXT'}

'''
Create a sqlalchemy engine for mysql or sql server
'''

#mysql
# def mysql_engine(user = myUSER, password = myPWD, host = myDB_HOST, port = myPORT, database = myDB):
#     engine = db.create_engine("mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, database))    
#     return engine

#sql server
def sql_engine(user = myUSER, password = myPWD, host = myDB_HOST, port = myPORT, database = myDB):
    engine = db.create_engine("mssql+pyodbc://{0}:{1}@{2}:{3}/{4}?driver=ODBC+Driver+17+for+SQL+Server".format(user, password, host, port, database))       
    return engine

'''
Create a sql connection from sqlalchemy engine
'''
def sql_conn(engine):
    conn = engine.raw_connection()
    return conn

'''
Create sql input for table names and types
'''
def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "pi_db_uid INT IDENTITY(1,1) PRIMARY KEY"
    df1 = df.rename(columns = {"" : "nocolname"})
    hdrs = df1.dtypes.index
    hdrs_list = [(hdr, str(df1[hdr].dtype)) for hdr in hdrs]
    for hl in hdrs_list:
        sql += " ,{0} {1}".format(hl[0], dmap[hl[1]])
    return sql

'''
Create a mysql table from a df
'''
def create_sql_tbl_schema(df, conn, db, tbl_name):
    tbl_cols_sql = gen_tbl_cols_sql(df)
    # for mysql
    # sql = "USE {0}; CREATE TABLE IF NOT EXISTS {1} ({2})".format(db, tbl_name, tbl_cols_sql)
    # for sql server
    sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{1}' and xtype='U') CREATE TABLE {1} ({2})".format(db, tbl_name, tbl_cols_sql)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.commit()

'''
Write df data to newly create mysql table
'''
def df_to_sql(df, engine, tbl_name):
    df.to_sql(tbl_name, engine, if_exists='replace')


'''
Execute API request and get response
'''
def api_call_request(type, api_endpoint, api_headers, data):
    if type == 'POST':
        response = requests.post(api_endpoint, headers=api_headers, data=data)
    elif type == 'GET':
        response = requests.get(api_endpoint, headers=api_headers, data=data)
    
    if response.status_code == 200:
        # response_data = response.json()
        response_data = json.loads(response.text)
    else:
        print("Error: ", response.status_code)
        response_data = ''
    
    return response_data