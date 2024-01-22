import pyodbc
import pandas as pandas
from getpass import getuser
import numpy as np
import pandas as pd
import tkinter as tk
import pyodbc
from sqlalchemy import create_engine
import datetime



"""
Created on Mon Jan 22 11:13:05 2024

@author: x947159
"""

# NOMBRE DEL ARCHIVO QUE SE QUIERE SEPARAR (input)
file = 'tablas.xlsx'

# NOMBRE DEL ARCHIVO QUE SERÁ RESULTADO DEL CÓDIGO
output = 'conteo_tablas_cd1.xlsx'

def as_pandas_DataFrame(cursor):
         names = [metadata[0] for metadata in cursor.description]
         return pandas.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)

username = getuser()         
cfg = {'DSN': 'Cloudera ODBC driver for Impala', 'host': 'corporacionimpala.aacc.gs.corp',
       'port': '21050','database' : 'default','username': username}

conn_string = '''DSN={0};Host={1};Port={2};Database={3};UID={4}'''.format(cfg['DSN'],cfg['host'],cfg['port'],cfg['database'],cfg['username'])

pyodbc.autocommit = True

conn = pyodbc.connect(conn_string, autocommit = True)

df = pd.read_excel(file, sheet_name='cd1')
lista_tabla=[]
lista_conteo=[]

for i in range(len(df['name'])):
    name = df['name'].iloc[i]
    # Configuration settings for the ODBC connection corporacionimpala.aacc.gs.corp


    #Fecha del dato
    #data_date_part="2023-02-28"
    print("leemos input")
    #file1='input_scoring_dec.xlsx'
    # # #engine=create_engine('sqlite://',echo=False)
    #df_input=pd.read_excel(file1,sheet_name='Hoja1')
    
    cursor = conn.cursor()
    
    # cursor.execute(f'''
    #               refresh cd_ifrs9_gcb_local.{name}
    #                 ''')
                    
    query_tabla=f'''
                  SELECT min(data_date_part) from cd_ifrs9_gcb_local.{name};
                    '''
    
    cursor.execute(query_tabla)
    
    df_tabla = as_pandas_DataFrame(cursor)
    
    lista_conteo.append(df_tabla)
    lista_tabla.append(name)

conteos=pd.DataFrame(lista_tabla) 
conteos.insert(1,'min_date_part',lista_conteo,allow_duplicates=True)


writer = pd.ExcelWriter(output, engine='xlsxwriter')

conteos.to_excel(writer,index=True,sheet_name='conteo')

# df_names_5493.to_excel(writer,index=True,sheet_name='5493')
# df_names_5505.to_excel(writer,index=True,sheet_name='5505')

writer.close()
