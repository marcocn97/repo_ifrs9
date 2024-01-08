####################################

import pyodbc
import pandas as pandas
from getpass import getuser
import numpy as np
import pandas as pd
import tkinter as tk
import pyodbc
from sqlalchemy import create_engine
import datetime


# Restablecer la variable global data_date_part a None

#IMPORTANTE: Cambiar a "NO" si no se quiere aplicar el filtro de +12 meses en AQUA+
#filtro_mes="SI"

def ejecutar_codigo():
    global filtro_seleccionado, datos_ingresados,resultado, data_date_part, filtro_mes
    filtro_seleccionado = combobox_filtro.get()
    datos_ingresados = []
    
    if filtro_seleccionado == "contract_id" or filtro_seleccionado == "customer_id":
        raw_datos = entrada_datos.get()
        # datos_limpio = raw_datos.replace("\n", ",").strip(",")
        # datos_ingresados = [dato.strip() for dato in datos_limpio.split(",")]
    data_date_part = entrada_fecha.get()
    filtro_mes=entrada_12_meses.get()

    ventana.destroy()

    #raw_datos = entrada_datos.get()
    datos_limpio = raw_datos.replace("\n", ",").strip(",")
    datos_ingresados = [dato.strip() for dato in datos_limpio.split(",")]
    
    #datos_ingresados = ['valor1', 'valor2', 'valor3']

    # Convertir los elementos de la lista con el formato deseado
    formatted_data = [f"'{valor}'," if valor != datos_ingresados[-1] else f"'{valor}'" for valor in datos_ingresados]
    
    # Unir los elementos en una cadena de texto
    resultado = ''.join(formatted_data)
    


# Crear la ventana principal
ventana = tk.Tk()
ventana.title("Cálculo de scoring_rating")

# Crear una etiqueta y un desplegable para seleccionar el filtro
etiqueta_filtro = tk.Label(ventana, text="Aplicar filtro sobre contrato/cliente:")
etiqueta_filtro.pack()

opciones_filtro = ["ninguno", "contract_id", "customer_id"]
combobox_filtro = tk.StringVar(ventana)
combobox = tk.OptionMenu(ventana, combobox_filtro, *opciones_filtro)
combobox_filtro.set(opciones_filtro[0])
combobox.pack()

# Crear una entrada de texto para ingresar los datos
entrada_datos = tk.Entry(ventana)
entrada_datos.pack()


# Crear una etiqueta y un campo de entrada para el data_date_part
etiqueta_fecha = tk.Label(ventana, text="Fecha (YYYY-MM-DD):")
etiqueta_fecha.pack()

entrada_fecha = tk.Entry(ventana)
entrada_fecha.pack()

# Crear una etiqueta y un campo de entrada para el filtro 12 meses
etiqueta_12_meses = tk.Label(ventana, text="Filtro 12 meses AQUA+. Escribir SI o NO:")
etiqueta_12_meses.pack()

entrada_12_meses=tk.Entry(ventana)
entrada_12_meses.pack()

# Crear un botón para ejecutar el código
boton_ejecutar = tk.Button(ventana, text="Ejecutar", command=ejecutar_codigo)
boton_ejecutar.pack()

# Iniciar el bucle principal de la interfaz
ventana.mainloop()

# Imprimir los resultados
print("Filtro seleccionado:", filtro_seleccionado)
#print(resultado)
print("Fecha:", data_date_part)

print(type(datos_ingresados))

contratos=pd.DataFrame(datos_ingresados,columns=['contratos'])

print(contratos)

# lista_resultado = resultado.split(',')
# df_filtro = pd.DataFrame(lista_resultado)

# contratos = df_filtro.to_string(index=False)

#print(contratos)

print("Fecha introducida: " + data_date_part)

#df_contratos = pd.DataFrame(datos_ingresados)
#print(df_contratos)

print("3")
def as_pandas_DataFrame(cursor):
         names = [metadata[0] for metadata in cursor.description]
         return pandas.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)
   



# Configuration settings for the ODBC connection corporacionimpala.aacc.gs.corp
username = getuser()         
cfg = {'DSN': 'Cloudera ODBC driver for Impala', 'host': 'corporacionimpala.aacc.gs.corp',
       'port': '21050','database' : 'default','username': username}

conn_string = '''DSN={0};Host={1};Port={2};Database={3};UID={4}'''.format(cfg['DSN'],cfg['host'],cfg['port'],cfg['database'],cfg['username'])

pyodbc.autocommit = True

conn = pyodbc.connect(conn_string, autocommit = True)

#Fecha del dato
#data_date_part="2023-02-28"
print("leemos input")
# file1='mensual_junio.xlsx'
# # #engine=create_engine('sqlite://',echo=False)
# df_input=pd.read_excel(file1,sheet_name='Sheet1')

cursor = conn.cursor()

cursor.execute('''
              refresh bu_ifrs9_gcb_core.ifrs9_input_motor_8_7
                ''')

cursor.execute('''
              refresh bu_ifrs9_gcb_local.stratus_ifrs9
               ''')
               

cursor.execute('''
              refresh cd_scib_internal_rating.aqp_rating_master_53
               ''')
   
cursor.execute('''
              refresh cd_scib_people_entities_clients.gbp_hist_current_ratings
               ''')

cursor.execute('''
              refresh bu_ifrs9_quants.tablon_acyg
               ''')

cursor.execute('''
              refresh st_ac.lake_static_bonds
               ''')

print('refreshes acabados')

#Descarga del timestamp correcto del input_motor (Parte FIC)
query_timestamp=f'''
              select data_timestamp_part
              from bu_ifrs9_gcb_core.ifrs9_input_motor_8_7
              where data_date_part='{data_date_part}'
              group by data_date_part,data_timestamp_part
              order by data_date_part,data_timestamp_part ASC
              LIMIT 1;
                '''
cursor.execute(query_timestamp)

df_timestamp = as_pandas_DataFrame(cursor)

timestamp=str(df_timestamp['data_timestamp_part'][0])
# timestamp="20230705205954"
# print(timestamp)

# if filtro_seleccionado=="contract_id":
# #Descarga del input_motor
#     query_input=f'''
#                   select contract_id,kgl4,customer_id,customer_segment_code,product_grouper,opening_date,opening_date_real,scoring_rating,initial_scoring_rating,current_guarantee_nominal_coverage,current_guarantee_coverage_percentage,data_date_part,data_timestamp_part
#                   from bu_ifrs9_gcb_core.ifrs9_input_motor_8_7
#                   where data_date_part='{data_date_part}'
#                   and data_timestamp_part='{timestamp}'
#                   and contract_id in ({resultado})
#                 '''
# else:
query_input=f'''
              select contract_id,kgl4,customer_id,customer_segment_code,product_grouper,opening_date,opening_date_real,scoring_rating,initial_scoring_rating,current_guarantee_nominal_coverage,current_guarantee_coverage_percentage,data_date_part,data_timestamp_part
              from bu_ifrs9_gcb_core.ifrs9_input_motor_8_7
              where data_date_part='{data_date_part}'
              and data_timestamp_part='{timestamp}'
          
            '''

cursor.execute(query_input)


df_input = as_pandas_DataFrame(cursor)

print('input acabado')
#Descarga de la tabla stratus_ifrs9
query_stratus=f'''
              select jcliente,cpty,parent,parentrating,origen,data_date_part
             from bu_ifrs9_gcb_local.stratus_ifrs9
             where data_date_part='{data_date_part}'
               '''

cursor.execute(query_stratus)
               

df_stratus = as_pandas_DataFrame(cursor)

print('stratus acabado')
#Descarga de la tabla aqp_rating_master_53
query_aqp=f'''
              select glcs_53,authorization_date_53,estado_rating_53,final_rating_53,data_date_part
             from cd_scib_internal_rating.aqp_rating_master_53
             where data_date_part='{data_date_part}'
             and estado_rating_53 not in ('PROPOSAL')
               '''

cursor.execute(query_aqp)

df_aqp_master = as_pandas_DataFrame(cursor)

print('aqp acabado')
##Descarga de la tabla gbp_hist_current_ratings (NO GCB)
query_gbp=f'''
              select entitycode,entityglobalrating,validity_start_date,data_date_part
              from cd_scib_people_entities_clients.gbp_hist_current_ratings
              ORDER BY validity_start_date desc
               '''


cursor.execute(query_gbp)

df_gbp_hist = as_pandas_DataFrame(cursor)
print('gbp acabado')
#Descarga de la tabla tablon_acyg (rating externos)
query_acyg=f'''
              select *
             from bu_ifrs9_quants.tablon_acyg
             where data_date_part='{data_date_part}'
               '''

cursor.execute(query_acyg)

df_cont_real = as_pandas_DataFrame(cursor)
print('cont real acabado')
##Descarga de la tabla lake_static_bonds (rating externos)
query_lake_static=f'''
              select *
             from st_ac.lake_static_bonds
             where data_date_part='{data_date_part}'
               '''

cursor.execute(query_lake_static)

df_lake_static = as_pandas_DataFrame(cursor)

print('lake acabado')
conn.close()

######################################
#Comienzo de tratamiento de los datos

# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 17:43:13 2022

@author: x947159
"""
#import sqlite3
import pandas as pd
#import numpy as np
from sqlalchemy import create_engine
import datetime
# from sqlalchemy.dialects.postgresql import insert
# from sqlalchemy import table, column
#from datetime import datetime, timedelta


#NOMBRE DEL ARCHIVO EXCEL DE SALIDA DE ESTE CÓDIGO
output=f'''scoring_rating_kerberos_{data_date_part}_v2.xlsx'''

engine=create_engine('sqlite://',echo=False)

#LECTURA EXCEL INPUT / En "sheet_name" indicar el nombre la hoja donde están los datos
#df_input=pd.read_excel(file,sheet_name='Hoja1')

#LECTURA EXCEL STRATUS / En "sheet_name" indicar el nombre la hoja donde están los datos
#df_stratus=pd.read_excel(file2,sheet_name='Hoja1')

############################
#Creación de listas para insertar los valores de cada columna
lista_initial=[]
lista_contract_id=[]
lista_kgl4=[]
lista_op_date_real=[]
lista_product_grouper=[]
lista_customer_segment_code=[]
lista_scoring_rating=[]
lista_customer_id=[]
lista_ast=[]
lista_cobertura=[]


for i in range(len(df_input['initial_scoring_rating'])):
    lista_ast.append('#')

df_input.insert(0,'ast',lista_ast,allow_duplicates=True)  
df_input['cobertura'] = df_input['ast'] + df_input['current_guarantee_coverage_percentage']

#PARTE DONDE SE DIVIDEN LOS CAMPOS QUE TIENEN SEPARACIÓN "#"

for i in range(len(df_input['initial_scoring_rating'])):
   
        isr=df_input['initial_scoring_rating'].iloc[i]
        k=df_input['kgl4'].iloc[i]
        odr=str(df_input['opening_date_real'].iloc[i])
        pg=str(df_input['product_grouper'].iloc[i])
        ci=str(df_input['contract_id'].iloc[i])
        csc=str(df_input['customer_segment_code'].iloc[i])
        sr=str(df_input['scoring_rating'].iloc[i])
        cid=str(df_input['customer_id'].iloc[i])
        cob=str(df_input['cobertura'].iloc[i])
        isr_2= isr.split("#")
        k_2=k.split("#")
        csc_2=csc.split('#')
        sr_2=sr.split('#')
        cid_2=cid.split('#')
        cob_2=cob.split('#')



#CADA CAMPO SE METE EN UNA LISTA
        lista_initial.append(isr_2)
        lista_contract_id.append(ci)
        lista_kgl4.append(k_2)
        lista_op_date_real.append(odr)
        lista_product_grouper.append(pg)
        lista_customer_segment_code.append(csc_2)
        lista_scoring_rating.append(sr_2)
        lista_customer_id.append(cid_2)
        lista_cobertura.append(cob_2)
        
print("CAMPOS SEPARADOS")

#CADA LISTA SE CONVIERTE EN UN DATAFRAME
kgl4_separado=pd.DataFrame(lista_kgl4)
segment_code_separado=pd.DataFrame(lista_customer_segment_code)
scoring_rating_separado=pd.DataFrame(lista_scoring_rating)
customer_id_separado=pd.DataFrame(lista_customer_id)
cobertura_separado=pd.DataFrame(lista_cobertura)
separacion_inicial=pd.DataFrame(lista_initial) 
separacion_inicial.insert(1,'c_id',lista_contract_id,allow_duplicates=True)
separacion_inicial.insert(2,'op_date',lista_op_date_real,allow_duplicates=True)
separacion_inicial.insert(3,'p_grouper',lista_product_grouper,allow_duplicates=True)



lista_final_initial=[]
lista_final_contract_id=[]
lista_tipo=[]
lista_final_kgl4=[]
lista_final_op_date_real=[]
lista_final_product_grouper=[]
lista_final_segment_code=[]
lista_final_scoring_rating=[]
lista_final_customer_id=[]
lista_final_cobertura=[]



#Número columnas del dataframe 
n_columnas=len(separacion_inicial.columns)

i=0
u=1

#print(row)
#FILAS DE TITULARES Y AVALISTAS SE POSICIONAN UNA DEBAJO DE OTRA
while i < (len(separacion_inicial[0])):
    if separacion_inicial[u][i]!=None:
          lista_final_initial.append(separacion_inicial[u][i])
          lista_final_contract_id.append(separacion_inicial['c_id'][i])
          lista_final_kgl4.append(kgl4_separado[u][i])
          lista_final_op_date_real.append(separacion_inicial['op_date'][i])
          lista_final_product_grouper.append(separacion_inicial['p_grouper'][i])
          lista_final_segment_code.append(segment_code_separado[u][i])
          lista_final_scoring_rating.append(scoring_rating_separado[u][i])
          lista_final_customer_id.append(customer_id_separado[u][i])
          lista_final_cobertura.append(cobertura_separado[u][i])
          if u==1:
              lista_tipo.append('titular')
          else:
              lista_tipo.append('avalista'+str(((u)-1)))
    i=i+1
    if i==(len(separacion_inicial[u])) and u<=n_columnas-5:
        i=0
        u=u+1


#AGRUPACIÓN DE COLUMNAS
df_separado=pd.DataFrame(lista_final_initial) 
df_separado.columns=['initial_scoring_rating']
df_separado.insert(0,'contract_id2',lista_final_contract_id,allow_duplicates=True)
df_separado.insert(2,'tipo',lista_tipo,allow_duplicates=True)
df_separado.insert(3,'kgl4',lista_final_kgl4,allow_duplicates=True)
df_separado.insert(4,'opening_date_real',lista_final_op_date_real,allow_duplicates=True)
df_separado.insert(5,'product_grouper',lista_final_product_grouper,allow_duplicates=True)
df_separado.insert(6,'customer_segment_code',lista_final_segment_code,allow_duplicates=True)
df_separado.insert(7,'scoring_rating',lista_final_scoring_rating,allow_duplicates=True)
df_separado.insert(8,'customer_id',lista_final_customer_id,allow_duplicates=True)
df_separado.insert(9,'cobertura',lista_final_cobertura,allow_duplicates=True)
df_separado['contract_id'] = df_separado.contract_id2.str.cat(df_separado.tipo)
col_contract_id2=df_separado.pop('contract_id')
df_separado.insert(1,'contract_id',col_contract_id2,allow_duplicates=True)

# df_separado=pd.merge(df_separado_1,contratos,left_on='contract_id',right_on='contratos',how='inner')
# df_separado.drop(['contratos'],axis=1,inplace=True)

print(type(df_separado['contract_id2']))
#Query filtrar contract_id
#planets[planets.year.isin([2008, 2009])]

if filtro_seleccionado=="contract_id":
    df_separado=df_separado.query(f'''contract_id2 in ({resultado})''')
    
elif filtro_seleccionado=="customer_id":
    df_separado=df_separado.query(f'''customer_id in ({resultado})''')

print(df_separado)

##############################################
#PARTE CÓDIGO GBP_HIST
##############################################

join_stratus=pd.merge(df_separado,df_stratus,left_on='customer_id',right_on='jcliente',how='left')

#join_stratus.to_excel('join_con_stratus.xlsx',index=False)

file3='gbp_hist.xlsx'
engine=create_engine('sqlite://',echo=False)
#df_gbp_hist=pd.read_excel(file3,sheet_name='Hoja1')


join_join_gbp=pd.merge(join_stratus,df_gbp_hist,left_on='cpty',right_on='entitycode',how='inner')
join_join_gbp=join_join_gbp.query('customer_segment_code=="08"')
join_join_gbp_v2=join_join_gbp.sort_values(by=['contract_id','validity_start_date'], ascending=False)  

horizontal_stack = pd.concat([join_stratus, df_gbp_hist], axis=1)

horizontal_stack.to_excel('horizontal.xlsx',index=False)

final_gbp=pd.DataFrame(join_join_gbp_v2,columns=horizontal_stack.columns) 
#final=final.dropna()
final_gbp=final_gbp.drop_duplicates(subset = "contract_id")

#union_tres_tablas=union_tres_tablas[['contract_id_syp','rating_elegido_proceso','comentario_proceso']]
final_gbp=final_gbp[['contract_id2','contract_id','initial_scoring_rating','tipo','kgl4','opening_date_real','product_grouper','customer_segment_code','scoring_rating','customer_id','validity_start_date','entitycode','entityglobalrating']]
final_solo_rating_gbp=final_gbp[['contract_id','entityglobalrating']]
final_solo_rating_gbp.columns=['contract_id_gbp','rating_gbp_hist_calculado']
final_solo_rating_gbp['rating_gbp_hist_calculado']=final_solo_rating_gbp['rating_gbp_hist_calculado'].str.replace(",", ".").astype(float)

#############################################
#PARTE CÓDIGO AQP-MASTER-53
#############################################

file4='aqp-master.xlsx'
engine=create_engine('sqlite://',echo=False)
#df_aqp_master=pd.read_excel(file4,sheet_name='Hoja1')
#JOIN DE INPUT+STRATUS CON AQP-MASTER
join_join_aqp=pd.merge(join_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

separacion=data_date_part.split(sep='-')
año_anterior=int(separacion[0])-1
fecha_filtro=str(año_anterior)+"-"+separacion[1]+"-"+"01"

join_join_aqp_v2=join_join_aqp.sort_values(by=['contract_id','authorization_date_53'], ascending=False)  

if filtro_mes=="SI":
    join_join_aqp_v2=join_join_aqp_v2.query(f"authorization_date_53 >='{fecha_filtro}'")

horizontal_stack = pd.concat([join_stratus, df_aqp_master], axis=1)

final_aqp=pd.DataFrame(join_join_aqp_v2,columns=horizontal_stack.columns) 
#final_aqp=final_aqp.dropna()

#############################################################################

final_aqp=final_aqp.drop_duplicates(subset = "contract_id")

#############################################################################
join_join_aqp_sin_filtrar=pd.merge(join_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

join_aqp_sin_filtrar_v2=join_join_aqp_sin_filtrar.sort_values(by=['contract_id','authorization_date_53'], ascending=False)  
horizontal_stack_2 = pd.concat([join_stratus, df_aqp_master], axis=1)

final_aqp_sin_filtrar=pd.DataFrame(join_aqp_sin_filtrar_v2,columns=horizontal_stack.columns) 
final_aqp_sin_filtrar=final_aqp_sin_filtrar.drop_duplicates(subset = "contract_id")


final_solo_rating_aqp_sin_filtrar=final_aqp_sin_filtrar[['contract_id','final_rating_53','authorization_date_53']]
final_solo_rating_aqp_sin_filtrar.columns=['contract_id_aqp','rating_aqua+_calculado','authorization_date_53']

final_solo_rating_aqp=final_aqp[['contract_id','final_rating_53','authorization_date_53']]
final_solo_rating_aqp.columns=['contract_id_aqp','rating_aqua+_calculado','authorization_date_53']

#final_solo_rating_aqp_sin_filtrar.to_excel('join_aqp-str.xlsx',index=True)


##############################################
#PARTE CÓDIGO PARENTRATING
##############################################
#join_stratus=pd.merge(filtrado2,df2,left_on='customer_id',right_on='jcliente',how='inner')
#join_stratus=join_stratus.drop(['cpty'],axis=1)

final_solo_rating_parentrating=join_stratus[['contract_id','parentrating']]
final_solo_rating_parentrating.columns=['contract_id_parentrating','rating_parentrating']
final_solo_rating_parentrating=final_solo_rating_parentrating.dropna()
#final_solo_rating_parentrating.to_excel('rating_parentrating.xlsx',index=False)

##############################################
#PARTE CÓDIGO RATING EXTERNOS
##############################################
file5= 'tablon_acyg.xlsx'

#LAKE_STATIC_BONDS
file6= 'lake_static_bonds.xlsx'

#TABLA MAPEO EXTERNOS
file7= 'mapeo_externos.xlsx'

engine=create_engine('sqlite://',echo=False)

#HOJA DEL EXCEL DONDE ESTÁN LOS DATOS PARA EL INPUT 
#df=pd.read_excel(file,sheet_name='Hoja1')

#HOJA DEL EXCEL DONDE ESTÁN LOS DATOS PARA CONTABILIDAD REAL
#df_cont_real=pd.read_excel(file5,sheet_name='Hoja1')

#HOJA DEL EXCEL DONDE ESTÁN LOS DATOS PARA LAKE STATIC BONDS
#df_lake_static=pd.read_excel(file6,sheet_name='Hoja1')

#HOJA DEL EXCEL DONDE ESTÁN LOS DATOS PARA MAPEO EXTERNOS
df_mapeo_ext=pd.read_excel(file7,sheet_name='Hoja1')

#LINEA DONDE SE UNE EL INPUT SEPARADO CON LA CONTABILIDAD REAL
#CAMBIAR EN "right_on" EL NOMBRE A "cxymode2_contrato_partenon"
join_contabilidad=pd.merge(df_separado,df_cont_real,left_on='contract_id2',right_on='ref_partenon',how='inner')

filtrado_join_contabilidad=join_contabilidad.query('tipo == "titular"')

#CAMBIAR en lef_on a "cxymode2_isin"
join_lake_static=pd.merge(filtrado_join_contabilidad,df_lake_static,left_on='isin',right_on='ac_isin',how='inner')


filtrado_lake=join_lake_static.iloc[:, [1,2,4,7,119,120,123]]

join_mapeo_moodys=pd.merge(filtrado_lake,df_mapeo_ext,left_on='bb_ra001_rtg_moody',right_on='Moodys',how='inner')
join_mapeo_syp=pd.merge(filtrado_lake,df_mapeo_ext,left_on='bb_ra002_rtg_sp',right_on='S&P',how='inner')
join_mapeo_fitch=pd.merge(filtrado_lake,df_mapeo_ext,left_on='bb_ra004_rtg_fitch',right_on='Fitch',how='inner')

lista_moodys=[]
lista_syp=[]
lista_fitch=[]
identificador_moodys=[]
identificador_syp=[]
identificador_fitch=[]

i=0
for i in range(len(join_mapeo_moodys['contract_id'])):
        
        if str(join_mapeo_moodys['customer_segment_code'][i])=="01":
       
            lista_moodys.append(join_mapeo_moodys['Sovereign'][i])
            identificador_moodys.append('Sovereign Moodys')
     
            
        elif str(join_mapeo_moodys['customer_segment_code'][i])=="02" or str(join_mapeo_moodys['customer_segment_code'][i])=="07": 
            lista_moodys.append(join_mapeo_moodys['Banks'][i])
            identificador_moodys.append('Banks Moodys')
         
            
        elif str(join_mapeo_moodys['customer_segment_code'][i])=="03" or str(join_mapeo_moodys['customer_segment_code'][i])=="04": 
            lista_moodys.append(join_mapeo_moodys['No Banks'][i])
            identificador_moodys.append('No Banks Moodys')
           
            
        elif str(join_mapeo_moodys['customer_segment_code'][i])=="05" or str(join_mapeo_moodys['customer_segment_code'][i])=="06" or str(join_mapeo_moodys['customer_segment_code'][i])=="08": 
            lista_moodys.append(join_mapeo_moodys['Corporates'][i])
            identificador_moodys.append('Corporates Moodys')
        
        i=i+1        
     
lista_contract_id=[]
i=0    

join_mapeo_moodys.insert(0,'rating_elegido',lista_moodys,allow_duplicates=True) 
join_mapeo_moodys.insert(1,'comentario',identificador_moodys,allow_duplicates=True)  


i=0
for i in range(len(join_mapeo_syp['contract_id'])):
      
        if str(join_mapeo_syp['customer_segment_code'][i])=="01":
           
            lista_syp.append(join_mapeo_syp['Sovereign'][i])
            identificador_syp.append('Sovereign S&P')
       
            
        elif str(join_mapeo_syp['customer_segment_code'][i])=="02" or str(join_mapeo_syp['customer_segment_code'][i])=="07": 
            lista_syp.append(join_mapeo_syp['Banks'][i])
            identificador_syp.append('Banks S&P')
           
            
        elif str(join_mapeo_syp['customer_segment_code'][i])=="03" or str(join_mapeo_syp['customer_segment_code'][i])=="04": 
            lista_syp.append(join_mapeo_syp['No Banks'][i])
            identificador_syp.append('No Banks S&P')
            
            
        elif str(join_mapeo_syp['customer_segment_code'][i])=="05" or str(join_mapeo_syp['customer_segment_code'][i])=="06" or str(join_mapeo_syp['customer_segment_code'][i])=="08": 
            lista_syp.append(join_mapeo_syp['Corporates'][i])
            identificador_syp.append('Corporates S&P')
          
            
join_mapeo_syp.insert(0,'rating_elegido',lista_syp,allow_duplicates=True) 
join_mapeo_syp.insert(1,'comentario',identificador_syp,allow_duplicates=True)  
#join_mapeo_syp.insert(2,'jerarquia',jerarquia_syp,allow_duplicates=True)  


i=0
for i in range(len(join_mapeo_fitch['contract_id'])):
        
        if str(join_mapeo_fitch['customer_segment_code'][i])=="01":
            lista_fitch.append(join_mapeo_fitch['Sovereign'][i])
            identificador_fitch.append('Sovereign Fitch')
        
            
        elif str(join_mapeo_fitch['customer_segment_code'][i])=="02" or str(join_mapeo_fitch['customer_segment_code'][i])=="07": 
            lista_fitch.append(join_mapeo_fitch['Banks'][i])
            identificador_fitch.append('Banks Fitch')
         
            
        elif str(join_mapeo_fitch['customer_segment_code'][i])=="03" or str(join_mapeo_fitch['customer_segment_code'][i])=="04": 
            lista_fitch.append(join_mapeo_fitch['No Banks'][i])
            identificador_fitch.append('No Banks Fitch')
           
            
        elif str(join_mapeo_fitch['customer_segment_code'][i])=="05" or str(join_mapeo_fitch['customer_segment_code'][i])=="06" or str(join_mapeo_fitch['customer_segment_code'][i])=="08": 
            lista_fitch.append(join_mapeo_fitch['Corporates'][i])
            identificador_fitch.append('Corporates Fitch')
  
            
join_mapeo_fitch.insert(0,'rating_elegido',lista_fitch,allow_duplicates=True) 
join_mapeo_fitch.insert(1,'comentario',identificador_fitch,allow_duplicates=True)  


combinacion = pd.concat([join_mapeo_moodys, join_mapeo_syp,join_mapeo_fitch], axis=1)

#combinacion.to_excel('combi.xlsx',index=False)

contratos_moodys=join_mapeo_moodys['contract_id']
contratos_syp=join_mapeo_syp['contract_id']
contratos_fitch=join_mapeo_fitch['contract_id']

vertical_stack = pd.concat([contratos_moodys,contratos_syp,contratos_fitch], axis=0)
vertical_stack.columns=['contract_id']
combi=pd.DataFrame(vertical_stack,columns=vertical_stack.columns)

combi_2=combi.drop_duplicates(subset = ['contract_id'])


#join contratos con moodys
join_con_moodys=pd.merge(combi_2,join_mapeo_moodys,left_on='contract_id',right_on='contract_id',how='left')
join_con_moodys=join_con_moodys[['contract_id','rating_elegido','comentario']]


#join contratos con syp
join_con_syp=pd.merge(combi_2,join_mapeo_syp,left_on='contract_id',right_on='contract_id',how='left')
join_con_syp=join_con_syp[['contract_id','rating_elegido','comentario']]


#join contratos con fitch
join_con_fitch=pd.merge(combi_2,join_mapeo_fitch,left_on='contract_id',right_on='contract_id',how='left')
join_con_fitch=join_con_fitch[['contract_id','rating_elegido','comentario']]

#join_con_moodys.to_excel('join_moodys.xlsx',index=False)
#join_con_syp.to_excel('join_syp.xlsx',index=False)
#join_con_fitch.to_excel('join_fitch.xlsx',index=False)

# union_dos_tablas=pd.merge(join_mapeo_moodys,join_mapeo_syp,left_on='contract_id',right_on='contract_id',how='outer')
# union_tres_tablas=pd.merge(union_dos_tablas,join_mapeo_fitch,left_on='contract_id',right_on='contract_id',how='outer')

union_tablas_r_externos = pd.merge(join_con_moodys, pd.merge(join_con_syp, join_con_fitch, on='contract_id', how='outer'), on='contract_id', how='outer')

#union_tres_tablas = pd.concat([join_con_moodys,join_con_syp,join_con_fitch], axis=1)
union_tablas_r_externos=union_tablas_r_externos.fillna(9999)

#union_tres_tablas.to_excel('union_tres.xlsx',index=False)


union_tablas_r_externos.columns=["contract_id","rating_elegido_moodys","comentario_moodys","rating_elegido_syp","comentario_syp","rating_elegido_fitch","comentario_fitch"]

#union_tres_tablas.to_excel('union_tres.xlsx',index=False)


lista_final_rating=[]
lista_final_comentario=[]
i=0
for i in range(len(union_tablas_r_externos['contract_id'])):

        if union_tablas_r_externos['rating_elegido_syp'][i]!=9999:
            lista_final_rating.append(union_tablas_r_externos['rating_elegido_syp'][i])
            lista_final_comentario.append(union_tablas_r_externos['comentario_syp'][i])
            
        else:
            lista_final_rating.append(min(float(union_tablas_r_externos['rating_elegido_moodys'][i]),float(union_tablas_r_externos['rating_elegido_fitch'][i])))
            if min(float(union_tablas_r_externos['rating_elegido_moodys'][i]),float(union_tablas_r_externos['rating_elegido_fitch'][i]))==float(union_tablas_r_externos['rating_elegido_moodys'][i]):
                lista_final_comentario.append(union_tablas_r_externos['comentario_moodys'][i])
            else:
                lista_final_comentario.append(union_tablas_r_externos['comentario_fitch'][i])
 
union_tablas_r_externos.insert(0,'rating_elegido_proceso',lista_final_rating,allow_duplicates=True) 
union_tablas_r_externos.insert(1,'comentario_proceso',lista_final_comentario,allow_duplicates=True) 

union_tablas_r_externos=union_tablas_r_externos[['contract_id','rating_elegido_proceso','comentario_proceso']]
union_tablas_r_externos.columns=['contract_id','rating_externo_elegido','comentario_elección']



rating_externos_completo=pd.merge(df_separado,union_tablas_r_externos,left_on='contract_id',right_on='contract_id',how='left')
#rating_externos_completo=rating_externos_completo.query('tipo == "titular"')

final_solo_rating_externos=rating_externos_completo[['contract_id','rating_externo_elegido']]
final_solo_rating_externos.columns=['contract_id_externos','rating_externo']

#######################################
#PARTE CÓDIGO AVALISTAS 100%
#######################################

query2=df_separado.query('tipo != "titular" and cobertura == "1.000000"')
cobertura=query2.sort_values(by=['contract_id2','scoring_rating'], ascending=False)
cobertura=cobertura.dropna()

query3=df_separado.query('tipo == "titular" and customer_segment_code != "08"')
avalado=query3.sort_values(by=['contract_id2'], ascending=False)

titular=pd.DataFrame(avalado,columns=df_separado.columns) 
join_cober=pd.merge(cobertura,titular,left_on='contract_id2',right_on='contract_id2',how='inner')

columnas_filtradas=join_cober[['contract_id_y', 'scoring_rating_y','tipo_y','customer_segment_code_y','contract_id_x','tipo_x','customer_segment_code_x','scoring_rating_x']]
columnas_filtradas.columns=['contract_id_titular','scoring_input','tipo','customer_segment_code_tit','contract_id_avalista','tipo_avalista','customer_segment_code_av','scoring_calculado']
final_solo_rating_100=columnas_filtradas.drop_duplicates(subset = "contract_id_titular")
final_solo_rating_100=final_solo_rating_100[['contract_id_titular','scoring_calculado']]
final_solo_rating_100.columns=['contract_id_av_100','rating_avalistas_100%']



#AGRUPACIÓN DE TODOS LOS RATINGS
join_gbp=pd.merge(df_separado,final_solo_rating_gbp,left_on='contract_id',right_on='contract_id_gbp',how='left')
join_final_aqp=pd.merge(join_gbp,final_solo_rating_aqp,left_on='contract_id',right_on='contract_id_aqp',how='left')
join_final_parentrating=pd.merge(join_final_aqp,final_solo_rating_parentrating,left_on='contract_id',right_on='contract_id_parentrating',how='left')
join_final_r_externos=pd.merge(join_final_parentrating,final_solo_rating_externos,left_on='contract_id',right_on='contract_id_externos',how='left')
join_final_rating_100=pd.merge(join_final_r_externos,final_solo_rating_100,left_on='contract_id',right_on='contract_id_av_100',how='left')
join_final_completo=pd.merge(join_final_rating_100,final_solo_rating_aqp_sin_filtrar,left_on='contract_id',right_on='contract_id_aqp',how='left')


join_final_completo=join_final_completo[['contract_id','tipo','customer_id','kgl4','customer_segment_code','scoring_rating','rating_gbp_hist_calculado','rating_aqua+_calculado_x','rating_parentrating','rating_externo','rating_avalistas_100%','authorization_date_53_y']]
join_final_completo.columns=['contract_id','tipo','customer_id','kgl4','customer_segment_code','scoring_rating','rating_gbp_hist_calculado','rating_aqua+_calculado','rating_parentrating','rating_externo','rating_avalistas_100%','authorization_date_53']


#join_final_5=join_final_5[['contract_id','tipo','customer_id','kgl4','customer_segment_code','scoring_rating','rating_gbp_hist_calculado','rating_aqua+_calculado','rating_parentrating','rating_externo','rating_avalistas_100%']]

#join_final_3.fillna(9999)

# join_aqp_con_parentrating=pd.merge(join_final_5,final_aqp,left_on='contract_id',right_on='contract_id',how='left')
# #join_aqp_con_parentrating=join_aqp_con_parentrating.fillna(9999)

# join_aqp_con_parentrating=join_aqp_con_parentrating.query('authorization_date_53 < "2022-01-01"')
# #join_aqp_con_parentrating=join_aqp_con_parentrating.dropna()
# join_aqp_con_parentrating=join_aqp_con_parentrating[['contract_id','authorization_date_53']]
# join_aqp_con_parentrating.to_excel('join_total_con_aqp.xlsx',index=False)

i=0
lista_eleccion=[]
lista_comentario=[]
for i in range(len(join_final_completo['contract_id'])):
    if  pd.isna(join_final_completo['rating_gbp_hist_calculado'][i])==False:
        lista_eleccion.append(join_final_completo['rating_gbp_hist_calculado'][i])
        lista_comentario.append('Rating gbp_hist')
    elif pd.isna(join_final_completo['rating_aqua+_calculado'][i])==False:
        lista_eleccion.append(join_final_completo['rating_aqua+_calculado'][i])
        lista_comentario.append('Rating aqua+')
    elif pd.isna(join_final_completo['rating_externo'][i])==False:
        lista_eleccion.append(join_final_completo['rating_externo'][i])
        lista_comentario.append('Rating externo')
    elif pd.isna(join_final_completo['rating_avalistas_100%'][i])==False:
        lista_eleccion.append(join_final_completo['rating_avalistas_100%'][i])
        lista_comentario.append('Rating avalistas 100%')
    elif pd.isna(join_final_completo['rating_parentrating'][i])==False and pd.isna(join_final_completo['authorization_date_53'][i])==True:
        lista_eleccion.append(join_final_completo['rating_parentrating'][i])
        lista_comentario.append('Rating parentrating+')
    elif pd.isna(join_final_completo['authorization_date_53'][i])==False and join_final_completo['customer_segment_code'][i]!="07":
         lista_eleccion.append(9999)
         lista_comentario.append('No encontrado / filtro +12 meses AQUA+')
    elif pd.isna(join_final_completo['authorization_date_53'][i])==False and join_final_completo['customer_segment_code'][i]=="07":
         lista_eleccion.append(9.3)
         lista_comentario.append('No encontrado segmento 07 / filtro +12 meses AQUA+')
    elif pd.isna(join_final_completo['rating_aqua+_calculado'][i])==True and pd.isna(join_final_completo['rating_externo'][i])==True and pd.isna(join_final_completo['rating_avalistas_100%'][i])==True and join_final_completo['customer_segment_code'][i]=='07':
         lista_eleccion.append(9.3)
         lista_comentario.append('No encontrado segmento 07')
   
    else:
        lista_eleccion.append(9999)
        lista_comentario.append('No encontrado')

        
join_final_completo.insert(12,'rating_calculado',lista_eleccion,allow_duplicates=True)
join_final_completo.insert(13,'Comentario_elección',lista_comentario,allow_duplicates=True)

lista_ok=[]

i=0
for i in range(len(join_final_completo['contract_id'])):
    if float(join_final_completo['scoring_rating'][i])==float(join_final_completo['rating_calculado'][i]) or ((float(join_final_completo['scoring_rating'][i])==1.00 or float(join_final_completo['scoring_rating'][i])==9999.00) and (float(join_final_completo['rating_calculado'][i])==0 or float(join_final_completo['rating_calculado'][i])==9999 or float(join_final_completo['rating_calculado'][i]==1.00))):
        lista_ok.append('OK rating')
    elif float(join_final_completo['scoring_rating'][i])==9999.00 and float(join_final_completo['rating_calculado'][i])==1.00:
        lista_ok.append('OK rating')
    else:
        lista_ok.append('NO OK rating')

join_final_completo.insert(14,'Coincide rating',lista_ok,allow_duplicates=True)

#Conteo de valores
clientes_sin_duplicados=join_final_completo.drop_duplicates(subset = "customer_id")
conteo_segmento=clientes_sin_duplicados['customer_segment_code'].value_counts()
print(conteo_segmento) 

#SE ESCRIBE LA HORA EN EL EXCEL
hora_actual = str(datetime.datetime.now())

hora_actual=hora_actual.replace(':','.')

#join_final_completo.to_excel(output,index=False,sheet_name=hora_actual)

writer = pd.ExcelWriter(output, engine='xlsxwriter')

# #CÓDIGO PARA GENERAR EL EXCEL
join_final_completo.to_excel(writer,index=False,sheet_name=hora_actual)
df_stratus.to_excel(writer,index=False,sheet_name='stratus')
df_aqp_master.to_excel(writer,index=False,sheet_name='aqp-master')
conteo_segmento.to_excel(writer,index=True,sheet_name='segmento')
df_cont_real.to_excel(writer,index=True,sheet_name='cont_real')
df_lake_static.to_excel(writer,index=True,sheet_name='lake_static_revisar_vacioss')

# df_names_5493.to_excel(writer,index=True,sheet_name='5493')
# df_names_5505.to_excel(writer,index=True,sheet_name='5505')

writer.close()



print(hora_actual)
print("Archivo Excel completado_v2 prueba master")