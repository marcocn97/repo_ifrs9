####################################

import pyodbc
import pandas as pd
import pandas
from getpass import getuser
import numpy as np
import sqlite3
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import table, column
from datetime import datetime, timedelta
import math



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
print("Comienza código")
#Fecha del dato
data_date_part="2023-12-04"

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
              refresh cd_scib_people_entities_clients.gbp_hist_historical_ratings
               ''')

print("Acaban refreshes")
# cursor.execute('''
#               refresh bu_ifrs9_quants.tablon_acyg
#                ''')

# cursor.execute('''
#               refresh st_ac.lake_static_bonds
#                ''')


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
#timestamp=20230613113854
print(timestamp)

#Descarga del input_motor
query_input=f'''
              select contract_id,kgl4,customer_id,customer_segment_code,product_grouper,opening_date,opening_date_real,scoring_rating,initial_scoring_rating,current_guarantee_nominal_coverage,current_guarantee_coverage_percentage,origen_perimetro,data_date_part,data_timestamp_part
             from bu_ifrs9_gcb_core.ifrs9_input_motor_8_7
             where data_date_part='{data_date_part}'
             and data_timestamp_part='{timestamp}'
               '''

cursor.execute(query_input)


df_input = as_pandas_DataFrame(cursor)

#Descarga de la tabla stratus_ifrs9
query_stratus=f'''
              select jcliente,cpty,parent,parentrating,origen,data_date_part
             from bu_ifrs9_gcb_local.stratus_ifrs9
             where data_date_part='{data_date_part}'
               '''

cursor.execute(query_stratus)
               

df_stratus = as_pandas_DataFrame(cursor)

#Descarga de la tabla aqp_rating_master_53
query_aqp=f'''
              select glcs_53,authorization_date_53,estado_rating_53,final_rating_53,data_date_part
             from cd_scib_internal_rating.aqp_rating_master_53
             where data_date_part='{data_date_part}'
             and estado_rating_53 not in ('PROPOSAL')
               '''

cursor.execute(query_aqp)

df_aqp_master = as_pandas_DataFrame(cursor)


##Descarga de la tabla gbp_hist_current_ratings (NO GCB)
query_gbp=f'''
              select entitycode,ratingdate,entityglobalrating,validity_start_date,finaldatevalidity,data_date_part
              from cd_scib_people_entities_clients.gbp_hist_historical_ratings
              order by validity_start_date desc
               '''


cursor.execute(query_gbp)

df_gbp_hist = as_pandas_DataFrame(cursor)

print("Fin querys")
###############################################################################
#CÓDIGO PARA PREPARACIÓN DEL INPUT
#data_date_part="2022-07-18"
output=f'''initial_rating_kerberos_{data_date_part}_bonos_no_bonos_y_gbp_bruto_completo_v12.xlsx'''

# file= 'input_motor_prueba.xlsx'

# df=pd.read_excel(file,sheet_name='Hoja1')

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
df_input['cobertura'] = df_input.ast.str.cat(df_input.current_guarantee_coverage_percentage)

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

#Elimino jcliente y parentrating de mi tabla
join_stratus=pd.merge(df_separado,df_stratus,left_on='customer_id',right_on='jcliente',how='left')
join_stratus=join_stratus.drop(['cpty'],axis=1)

#join_stratus.to_excel("unión_input_stratus",index=False)

#GBP_HIST
join_join_gbp=pd.merge(join_stratus,df_gbp_hist,left_on='kgl4',right_on='entitycode',how='left')
join_join_gbp=join_join_gbp.query('customer_segment_code=="08"')
join_join_gbp_v1=join_join_gbp.sort_values(by=['contract_id','validity_start_date'], ascending=False) 

#Bucle donde se asigna indicador para detectar validity más reciente
lista_fecha=[]
i=0
for i in range(len(join_join_gbp_v1['contract_id'])):
    if join_join_gbp_v1['contract_id'].iloc[0]:
        lista_fecha.append("0")
    elif join_join_gbp_v1['contract_id'].iloc[i]!=join_join_gbp_v1['contract_id'].iloc[i-1]:
        lista_fecha.append("0")
    elif join_join_gbp_v1['contract_id'].iloc[i]==join_join_gbp_v1['contract_id'].iloc[i-1] and join_join_gbp_v1['validity_start_date'].iloc[i]==join_join_gbp_v1['validity_start_date'].iloc[i-1] and lista_fecha[i-1]=="0":
        lista_fecha.append("0")
    else:
        lista_fecha.append("1")

#Filtramos por fecha de validity más reciente para cada cliente
join_join_gbp_v1.insert(0,'indicador_fecha',lista_fecha,allow_duplicates=True)
join_join_gbp_v2=join_join_gbp_v1.drop(join_join_gbp_v1[(join_join_gbp_v1['indicador_fecha']=="1")].index)


#Listas para dtaframe final
i=0
lista_gbp_historical=[]
lista_entitycode=[]
lista_ratingdate=[]
lista_op_real=[]
lista_finaldate=[]
lista_contrato=[]
lista_prioridad=[]
lista_validity=[]
cercania_rating_date=[]
lista_init_input=[]

#Rellenar con una fecha aleatoria para que los kgl4 que no tengan finaldatevalidity
#no den error
#join_join_gbp_v2['ratingdate']=join_join_gbp_v2['ratingdate'][0:18]
join_join_gbp_v2['finaldatevalidity']=join_join_gbp_v2['finaldatevalidity'].fillna('9999-12-31 00:00:00')
join_join_gbp_v2['ratingdate']=join_join_gbp_v2['ratingdate'].fillna('9999-12-31 00:00:00')

#Orden de prioridad de entityglobalrating según DDP
for i in range(len(join_join_gbp_v2['contract_id'])):
            
            print(i)
            try:
                data_date_part_2=datetime.strptime(str(data_date_part),'%Y-%m-%d')
                x=datetime.strptime(str(join_join_gbp_v2['opening_date_real'].iloc[i]),'%Y-%m-%d')
                if len(join_join_gbp_v2['ratingdate'].iloc[i])>19:
                    y=datetime.strptime(str(join_join_gbp_v2['ratingdate'].iloc[i]),'%Y-%m-%d %H:%M:%S.%fZ')
                else:
                    y=datetime.strptime(str(join_join_gbp_v2['ratingdate'].iloc[i]),'%Y-%m-%d %H:%M:%S')
                
                z=datetime.strptime(str(join_join_gbp_v2['finaldatevalidity'].iloc[i]),'%Y-%m-%d %H:%M:%S')
                validity=datetime.strptime(str(join_join_gbp_v2['validity_start_date'].iloc[i]),'%Y-%m-%d %H:%M:%S')
                dias_1 = float((z - x) / timedelta(days=1))
                dias_2 = float((x - y) / timedelta(days=1))
                
                if data_date_part_2>validity and dias_1>=0.0 and dias_2>=0.0:
                        lista_gbp_historical.append(join_join_gbp_v2['entityglobalrating'].iloc[i])
                        lista_entitycode.append(join_join_gbp_v2['entitycode'].iloc[i])
                        lista_ratingdate.append(join_join_gbp_v2['ratingdate'].iloc[i])
                        lista_op_real.append(join_join_gbp_v2['opening_date_real'].iloc[i])
                        lista_finaldate.append(join_join_gbp_v2['finaldatevalidity'].iloc[i])
                        lista_contrato.append(join_join_gbp_v2['contract_id'].iloc[i])
                        lista_init_input.append(join_join_gbp_v2['initial_scoring_rating'].iloc[i])
                        lista_validity.append(join_join_gbp_v2['validity_start_date'].iloc[i])
                        cercania_rating_date.append(abs(float((x - y) / timedelta(days=1))))
                        lista_prioridad.append("1")
                        
                elif data_date_part_2>validity and dias_2<=365.0 and dias_2>=0.0:
                        lista_gbp_historical.append(join_join_gbp_v2['entityglobalrating'].iloc[i])
                        lista_entitycode.append(join_join_gbp_v2['entitycode'].iloc[i])
                        lista_ratingdate.append(join_join_gbp_v2['ratingdate'].iloc[i])
                        lista_op_real.append(join_join_gbp_v2['opening_date_real'].iloc[i])
                        lista_finaldate.append(join_join_gbp_v2['finaldatevalidity'].iloc[i])
                        lista_contrato.append(join_join_gbp_v2['contract_id'].iloc[i])
                        lista_init_input.append(join_join_gbp_v2['initial_scoring_rating'].iloc[i])
                        lista_validity.append(join_join_gbp_v2['validity_start_date'].iloc[i])
                        cercania_rating_date.append(abs(float((x - y) / timedelta(days=1))))
                        lista_prioridad.append("2")
                        
                elif data_date_part_2>validity and dias_2<=0:
                        lista_gbp_historical.append(join_join_gbp_v2['entityglobalrating'].iloc[i])
                        lista_entitycode.append(join_join_gbp_v2['entitycode'].iloc[i])
                        lista_ratingdate.append(join_join_gbp_v2['ratingdate'].iloc[i])
                        lista_op_real.append(join_join_gbp_v2['opening_date_real'].iloc[i])
                        lista_finaldate.append(join_join_gbp_v2['finaldatevalidity'].iloc[i])
                        lista_contrato.append(join_join_gbp_v2['contract_id'].iloc[i])
                        lista_init_input.append(join_join_gbp_v2['initial_scoring_rating'].iloc[i])
                        lista_validity.append(join_join_gbp_v2['validity_start_date'].iloc[i])
                        cercania_rating_date.append(abs(float((x - y) / timedelta(days=1))))
                        lista_prioridad.append("3")
                        
                elif data_date_part_2>validity and dias_2>365:
                        lista_gbp_historical.append(join_join_gbp_v2['entityglobalrating'].iloc[i])
                        lista_entitycode.append(join_join_gbp_v2['entitycode'].iloc[i])
                        lista_ratingdate.append(join_join_gbp_v2['ratingdate'].iloc[i])
                        lista_op_real.append(join_join_gbp_v2['opening_date_real'].iloc[i])
                        lista_finaldate.append(join_join_gbp_v2['finaldatevalidity'].iloc[i])
                        lista_contrato.append(join_join_gbp_v2['contract_id'].iloc[i])
                        lista_init_input.append(join_join_gbp_v2['initial_scoring_rating'].iloc[i])
                        lista_validity.append(join_join_gbp_v2['validity_start_date'].iloc[i])
                        cercania_rating_date.append(abs(float((x - y) / timedelta(days=1))))
                        lista_prioridad.append("4")
                else:
                    print("nan")
            except ValueError:
                print(".")
#Agrupación listas en dataframe
df_str_gbp=pd.DataFrame(lista_gbp_historical) 
df_str_gbp.columns=['entityglobalrating']
df_str_gbp.insert(1,'initial_input',lista_init_input,allow_duplicates=True)
df_str_gbp.insert(2,'contract_id',lista_contrato,allow_duplicates=True)
df_str_gbp.insert(3,'entitycode',lista_entitycode,allow_duplicates=True)
df_str_gbp.insert(4,'ratingdate',lista_ratingdate,allow_duplicates=True)  
df_str_gbp.insert(5,'opening_date_real',lista_op_real,allow_duplicates=True)  
df_str_gbp.insert(6,'finaldatevalidity',lista_finaldate,allow_duplicates=True) 
df_str_gbp.insert(7,'cercanía_ratingdate',cercania_rating_date,allow_duplicates=True) 
df_str_gbp.insert(8,'prioridad',lista_prioridad,allow_duplicates=True) 
df_str_gbp.insert(9,'validity_start_date',lista_validity,allow_duplicates=True) 
df_str_gbp=df_str_gbp.sort_values(by=['contract_id','prioridad','cercanía_ratingdate','entityglobalrating'], ascending=[True,True,True,False])   
df_str_gbp=df_str_gbp.drop_duplicates(subset = "contract_id") 


#CÓDIGO PARA NO BONOS AQP-MASTER

###############################################################################
#CÓDIGO REFERENTE A DIF_POSITIVO
print("COMIENZA CÓDIGO DIF POSITIVA")

# file3= 'aqp-master.xlsx'
# engine=create_engine('sqlite://',echo=False)
# df3=pd.read_excel(file3,sheet_name='Hoja1')
#join_stratus.to_sql('prueba_v2',engine,if_exists='replace',index=False)
#df3.to_sql('prueba_v3',engine,if_exists='replace',index=False)

print("Ficheros aqp_master e input to sql")

# results=engine.execute("Select * from prueba_v2 \
#                        LEFT JOIN prueba_v3 ON prueba_v2.parent = prueba_v3.glcs_53\
#                         ORDER BY prueba_v2.contract_id")

join_join_aqp=pd.merge(join_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

join_join_aqp_v2=join_join_aqp.sort_values(by=['contract_id'], ascending=False)  

# horizontal_stack = pd.concat([join_stratus, df_stratus], axis=1)

# final=pd.DataFrame(results,columns=horizontal_stack.columns) 
join_join_aqp_v2=join_join_aqp_v2.dropna(subset=['parent','glcs_53'])

print("Simulación sql y convertidos en dataframe")

#############################################################################
lista=[]
i=0
for i in range(len(join_join_aqp_v2['opening_date_real'])):
        print(i)
        x=datetime.strptime(str(join_join_aqp_v2['opening_date_real'].iloc[i]),'%Y-%m-%d')
        y=datetime.strptime(str(join_join_aqp_v2['authorization_date_53'].iloc[i]),'%Y-%m-%d %H:%M:%S.%f')
        i=i+1
     
        dias = float((x - y) / timedelta(days=1))
        dias_redondeado=math.ceil(dias)
        lista.append(int(dias_redondeado))
print("Diferencias op_date y auth_date")

join_join_aqp_v2.insert(12,'diff_op_date_authorization',lista,allow_duplicates=True)
print("Insertamos en el dataframe la columna de diferencias")

#join_join_aqp_v2.to_sql('prueba9',engine,if_exists='replace',index=False)    


#DIFERENCIAS MAYOR QUE 0 
# results_2=engine.execute("Select *\
#                         from prueba9 where diff_op_date_authorization>=0\
#                         ORDER BY contract_id,diff_op_date_authorization") 
           
join_join_aqp_v2=join_join_aqp_v2.query('diff_op_date_authorization>=0')
#join_join_aqp_v2=pd.merge(df_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

join_join_aqp_v3=join_join_aqp_v2.sort_values(by=['contract_id','diff_op_date_authorization'], ascending=True)  


                       
print("Ordenamos contratos por dif_positiva")                      
#final_2=pd.DataFrame(results_2,columns=final.columns)

track=pd.DataFrame()


positivo=join_join_aqp_v3.drop_duplicates(subset = "contract_id")
print("Filtramos por el contrato con dif_positivo más cercano a 0")      


#positivo.to_excel('archivo_positivos.xlsx',index=False)

################################################################################
#CÓDIGO REFERENTE A DIF_NEGATIVO
print("COMIENZA CÓDIGO DIF_NEGATIVA")      

file3= 'aqp-master.xlsx'

# engine=create_engine('sqlite://',echo=False)
# df5=pd.read_excel(file3,sheet_name='Hoja1')
# join_stratus.to_sql('prueba_v4',engine,if_exists='replace',index=False)
#df3.to_sql('prueba_v5',engine,if_exists='replace',index=False)

print("Ficheros aqp_master e input to sql")

# results3=engine.execute("Select * from prueba_v4 \
#                         LEFT JOIN prueba_v5 ON prueba_v4.parent = prueba_v5.glcs_53\
#                         ORDER BY prueba_v4.contract_id")

join_join_aqp_neg=pd.merge(join_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

join_join_aqp_neg2=join_join_aqp_neg.sort_values(by=['contract_id'], ascending=False)  
# horizontal_stack = pd.concat([join_stratus, df3], axis=1)

# final=pd.DataFrame(results3,columns=horizontal_stack.columns) 
join_join_aqp_neg2=join_join_aqp_neg2.dropna(subset=['parent','glcs_53'])

print("Simulación sql y convertidos en dataframe")

#############################################################################
lista=[]
i=0
for i in range(len(join_join_aqp_neg2['opening_date_real'])):
        try:
            x=datetime.strptime(str(join_join_aqp_neg2['opening_date_real'].iloc[i]),'%Y-%m-%d')
            y=datetime.strptime(str(join_join_aqp_neg2['authorization_date_53'].iloc[i]),'%Y-%m-%d %H:%M:%S.%f')
            i=i+1
            dias = float((x - y) / timedelta(days=1))
            dias_redondeado=math.ceil(dias)
            lista.append(int(dias_redondeado))
        except ValueError:
            print('nan')
        
print("Diferencias op_date y auth_date")

join_join_aqp_neg2.insert(12,'diff_op_date_authorization',lista,allow_duplicates=True)
print("Insertamos en el dataframe la columna de diferencias")

#final.to_sql('prueba10',engine,if_exists='replace',index=False)    


#DIFERENCIAS MENOR QUE 0 
# results_2=engine.execute("Select * from prueba10 where diff_op_date_authorization<0\
#                         ORDER BY contract_id,diff_op_date_authorization desc")

join_join_aqp_neg2=join_join_aqp_neg2.query('diff_op_date_authorization<0')
#join_join_aqp_v2=pd.merge(df_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')

join_join_aqp_neg3=join_join_aqp_neg2.sort_values(by=['contract_id','diff_op_date_authorization'], ascending=False)  

         
# print("Ordenamos contratos por dif_negativa")                       
# final_3=pd.DataFrame(results_2,columns=final.columns)

negativo=join_join_aqp_neg3.drop_duplicates(subset = "contract_id")
print("Filtramos por el contrato con dif_negativo más cercano a 0")       

#negativo.to_excel('archivo_negativos.xlsx',index=False)

###########################################################################
# negativo.to_sql('negativo',engine,if_exists='replace',index=False)


# positivo.to_sql('positivo',engine,if_exists='replace',index=False)


# #UNIÓN DE ARCHIVOS POSITIVO Y NEGATIVO
# results5=engine.execute("Select positivo.contract_id,positivo.tipo,positivo.product_grouper,positivo.customer_segment_code,positivo.initial_scoring_rating,positivo.diff_op_date_authorization,positivo.final_rating_53,positivo.parent,negativo.contract_id,negativo.tipo,negativo.product_grouper,negativo.customer_segment_code,negativo.initial_scoring_rating,negativo.diff_op_date_authorization,negativo.final_rating_53,negativo.parent from positivo \
#                         LEFT JOIN negativo ON positivo.contract_id = negativo.contract_id\
#                            UNION ALL\
#                         Select positivo.contract_id,positivo.tipo,positivo.product_grouper,positivo.customer_segment_code,positivo.initial_scoring_rating,positivo.diff_op_date_authorization,positivo.final_rating_53,positivo.parent,negativo.contract_id,negativo.tipo,negativo.product_grouper,negativo.customer_segment_code,negativo.initial_scoring_rating,negativo.diff_op_date_authorization,negativo.final_rating_53,negativo.parent from negativo \
#                           LEFT JOIN positivo ON negativo.contract_id = positivo.contract_id      where positivo.contract_id IS NULL\
#                    ")
                       
# print("Unión de archivos positivo y negativo")
positivo=positivo[['contract_id','tipo','product_grouper','customer_segment_code','initial_scoring_rating','diff_op_date_authorization','final_rating_53','parent']]
negativo=negativo[['contract_id','tipo','product_grouper','customer_segment_code','initial_scoring_rating','diff_op_date_authorization','final_rating_53','parent']]
union_pos_neg=pd.merge(positivo,negativo,left_on='contract_id',right_on='contract_id',how='outer')

horizontal_stack = pd.concat([positivo, negativo], axis=1)

#final4=pd.DataFrame(results5) 

filtrado=union_pos_neg.fillna(0)

lista_2=[]
lista_rating=[]
i=0


filtrado.columns=["contract_id","tipo","product_grouper","customer_segment_code","initial_scoring_rating","positivo_diff_op_date_authorization","final_rating_53","parent","tipo2","product_grouper2","customer_segment_code2","initial_scoring_rating2","negativo_diff_op_date_authorization","final_rating_532","parent2"]

#filtrado.to_excel("previo_uat.xlsx",index=False)

for i in range(len(filtrado['contract_id'])):
        if 0<= int(filtrado["positivo_diff_op_date_authorization"][i])<=365 and filtrado["tipo"][i]!=0:
  
            lista_2.append(float(filtrado['final_rating_53'][i]))
            lista_rating.append("rating positivo")
        elif str(filtrado['tipo2'][i])=="0":
            lista_2.append(float(filtrado['final_rating_53'][i]))
            lista_rating.append("rating positivo")
        elif abs(int(filtrado["positivo_diff_op_date_authorization"][i]))<abs(int(filtrado["negativo_diff_op_date_authorization"][i])):
            if filtrado['tipo'][i]!=0:
                lista_2.append(float(filtrado['final_rating_53'][i]))
                lista_rating.append("rating positivo")
            else:
                lista_2.append(float(filtrado['final_rating_532'][i]))
                lista_rating.append("rating negativo") 
        else: 
            lista_2.append(float(filtrado['final_rating_532'][i]))
            lista_rating.append("rating negativo")
        i=i+1
lista_contract_id=[]
i=0        
for i in range(len(filtrado['contract_id'])):
                #filtrado['contract_id'][i]!=0
                lista_contract_id.append(filtrado['contract_id'][i])
                i=i+1
                # else:
                #     lista_contract_id.append(filtrado['contract_id2'][i]) 
                    

filtrado.insert(15,'rating_calculado',lista_2,allow_duplicates=True)
filtrado.insert(16,'contract_id3',lista_contract_id,allow_duplicates=True)
filtrado.insert(17,'elección_rating',lista_rating,allow_duplicates=True)



i=0
lista_dif=[]
for i in range(len(filtrado['contract_id'])):
    if filtrado['elección_rating'][i]=="rating positivo":
        lista_dif.append(float(filtrado['initial_scoring_rating'][i])-float(filtrado['rating_calculado'][i]))
    else:
        lista_dif.append(float(filtrado['initial_scoring_rating2'][i])-float(filtrado['rating_calculado'][i]))
        
filtrado.insert(18,'diferencia',lista_dif,allow_duplicates=True)



print("Rating elegido por dif_positiva o dif_negativa")


filtrado= filtrado.drop(filtrado[(filtrado['tipo'] == 'titular') & (filtrado['product_grouper'] == 'AA')].index)
filtrado= filtrado.drop(filtrado[(filtrado['tipo'] == 'titular') & (filtrado['product_grouper'] == 'RF')].index)

filtrado= filtrado.drop(filtrado[(filtrado['tipo2'] == 'titular') & (filtrado['product_grouper2'] == 'AA')].index)
filtrado= filtrado.drop(filtrado[(filtrado['tipo2'] == 'titular') & (filtrado['product_grouper2'] == 'RF')].index)
filtrado=filtrado.drop(['contract_id'],axis=1)
col_contract_id3=filtrado.pop('contract_id3')
filtrado.insert(0,'contract_id3',col_contract_id3,allow_duplicates=True)

###############################################################################
#PARTE CÓDIGO BONOS
#join_join_aqp=pd.merge(join_stratus,df_aqp_master,left_on='parent',right_on='glcs_53',how='left')
#join_join_gbp=join_join_aqp.query((join_join_aqp[(join_join_aqp['tipo'] == 'titular') & (join_join_aqp['product_grouper'] == 'AA')].index))
#join_join_gbp=join_join_aqp.query((join_join_aqp[(join_join_aqp['tipo'] == 'titular') & (join_join_aqp['product_grouper'] == 'RF')].index))

join_join_aqp_bonos=join_join_aqp.query('tipo=="titular" and (product_grouper=="AA" or product_grouper=="RF")')



#filtrado= filtrado.drop(filtrado[(filtrado['tipo'] == 'titular') & (filtrado['product_grouper'] == 'AA')].index)
#join_join_gbp_v2=join_join_gbp.sort_values(by=['contract_id','validity_start_date'], ascending=False)
join_join_aqp_bonos_v2=join_join_aqp_bonos.sort_values(by=['contract_id'], ascending=True)

#horizontal_stack = pd.concat([df, df2], axis=1)
 
# final=pd.DataFrame(results,columns=horizontal_stack.columns) 
###join_join_aqp_bonos_v2=join_join_aqp_bonos_v2.dropna()
join_join_aqp_bonos_v2 = join_join_aqp_bonos_v2.dropna(subset=['contract_id','authorization_date_53'])
# print(final)
# final.to_excel(output,index=False)
print("Simulación sql y convertidos en dataframe")

#############################################################################
lista=[]
i=0
for i in range(len(join_join_aqp_bonos_v2['opening_date_real'])):
        try:
            x=datetime.strptime(str(join_join_aqp_bonos_v2['opening_date_real'].iloc[i]),'%Y-%m-%d')
            y=datetime.strptime(str(join_join_aqp_bonos_v2['authorization_date_53'].iloc[i]),'%Y-%m-%d %H:%M:%S.%f')
            i=i+1
            dias = float((x - y) / timedelta(days=1))
            dias_redondeado=math.ceil(dias)
            lista.append(int(dias_redondeado))
        except ValueError:
            print('nan')
            
print("Diferencias op_date y auth_date")

join_join_aqp_bonos_v2.insert(12,'diff_op_date_authorization',lista,allow_duplicates=True)
print("Insertamos en el dataframe la columna de diferencias")



join_join_aqp_bonos_v2_pos=join_join_aqp_bonos_v2.query('diff_op_date_authorization>0')    
join_join_aqp_bonos_v2_pos_v2=join_join_aqp_bonos_v2_pos.sort_values(by=['contract_id','diff_op_date_authorization'], ascending=True)                    

print("Ordenamos contratos por dif_positiva")                      
# final_2=pd.DataFrame(results_2,columns=final.columns)



positivo_bonos=join_join_aqp_bonos_v2_pos_v2.drop_duplicates(subset = "contract_id")
print("Filtramos por el contrato con dif_positivo más cercano a 0")      

positivo_bonos=positivo_bonos[['contract_id2','contract_id','tipo','product_grouper','initial_scoring_rating','diff_op_date_authorization','parent','final_rating_53']]
positivo_bonos.columns=['contract_id2pos','contract_id_pos','tipo_pos','product_grouper_pos','initial_scoring_rating_pos','diff_op_date_authorization_pos','parent','rating_pos']

###########################################################
#filtrado positivo


print("COMIENZA CÓDIGO DIF POSITIVA")


print("Ficheros aqp_master e input to sql")


join_join_aqp_bonos_v2_neg=join_join_aqp_bonos_v2.query('diff_op_date_authorization<=0')    
join_join_aqp_bonos_v2_neg_v2=join_join_aqp_bonos_v2_neg.sort_values(by=['contract_id','final_rating_53'], ascending=False)   
                       
print("Ordenamos contratos por dif_positiva")                      
#final_2=pd.DataFrame(results_2,columns=final.columns)

 
negativo_bonos=join_join_aqp_bonos_v2_neg_v2.drop_duplicates(subset = "contract_id")
print("Filtramos por el contrato con dif_positivo más cercano a 0")      

negativo_bonos=negativo_bonos[['contract_id2','contract_id','tipo','product_grouper','initial_scoring_rating','diff_op_date_authorization','parent','final_rating_53']]
negativo_bonos.columns=['contract_id2neg','contract_id_neg','tipo_neg','product_grpuper_neg','initial_scoring_rating_neg','diff_op_date_authorization_neg','parent','rating_neg']

#Elección rating
#AGRUPACIÓN DE TODOS LOS RATINGS
join_negativo_bonos=pd.merge(df_separado,negativo_bonos,left_on='contract_id',right_on='contract_id_neg',how='left')
join_final_completo_bonos=pd.merge(join_negativo_bonos,positivo_bonos,left_on='contract_id',right_on='contract_id_pos',how='left')

join_final_completo_bonos=join_final_completo_bonos[['contract_id','tipo','product_grouper','customer_id','kgl4','customer_segment_code','initial_scoring_rating','rating_neg','rating_pos','diff_op_date_authorization_pos','diff_op_date_authorization_neg']]
#join_final_completo.columns=['contract_id','tipo','customer_id','kgl4','customer_segment_code','scoring_rating','rating_gbp_hist_calculado','rating_aqua+_calculado','rating_parentrating','rating_externo','rating_avalistas_100%','authorization_date_53']


i=0
lista_eleccion=[]
lista_comentario=[]
lista_dias_dif=[]
for i in range(len(join_final_completo_bonos['contract_id'])):
    if  pd.isna(join_final_completo_bonos['rating_neg'][i])==False:
        lista_eleccion.append(join_final_completo_bonos['rating_neg'][i])
        lista_dias_dif.append(join_final_completo_bonos['diff_op_date_authorization_neg'][i])
        lista_comentario.append('bono / rating negativo')
    elif pd.isna(join_final_completo_bonos['rating_pos'][i])==False:
        lista_eleccion.append(join_final_completo_bonos['rating_pos'][i])
        lista_dias_dif.append(join_final_completo_bonos['diff_op_date_authorization_pos'][i])
        lista_comentario.append('bono / rating_positivo')
    else:
        lista_eleccion.append(9999)
        lista_comentario.append('No encontrado')
        lista_dias_dif.append('-')

join_final_completo_bonos.insert(11,'rating_calculado',lista_eleccion,allow_duplicates=True)
join_final_completo_bonos.insert(12,'Comentario_eleción',lista_comentario,allow_duplicates=True)
join_final_completo_bonos.insert(13,'Dif_días',lista_dias_dif,allow_duplicates=True)

lista_ok=[]

i=0
for i in range(len(join_final_completo_bonos['contract_id'])):
    if float(join_final_completo_bonos['initial_scoring_rating'][i])==float(join_final_completo_bonos['rating_calculado'][i]):
        lista_ok.append('OK rating')
    else:
        lista_ok.append('NO OK rating')

join_final_completo_bonos.insert(14,'Coincide rating',lista_ok,allow_duplicates=True)

join_final_completo_bonos=join_final_completo_bonos.drop(['diff_op_date_authorization_neg','diff_op_date_authorization_pos'],axis=1)

#PARTE CÓDIGO AVALISTAS 100%
#######################################

query_av=df_separado.query('tipo != "titular" and cobertura == "1.000000"')
cobertura=query_av.sort_values(by=['contract_id2','initial_scoring_rating'], ascending=False)
cobertura=cobertura.dropna()

query3=df_separado.query('tipo == "titular" and customer_segment_code != "08"')
avalado=query3.sort_values(by=['contract_id2'], ascending=False)

titular=pd.DataFrame(avalado,columns=df_separado.columns) 
join_cober=pd.merge(cobertura,titular,left_on='contract_id2',right_on='contract_id2',how='inner')

columnas_filtradas=join_cober[['contract_id_y', 'initial_scoring_rating_y','tipo_y','customer_segment_code_y','contract_id_x','tipo_x','customer_segment_code_x','initial_scoring_rating_x']]
columnas_filtradas.columns=['contract_id_titular','initial_scoring_input','tipo','customer_segment_code_tit','contract_id_avalista','tipo_avalista','customer_segment_code_av','initial_scoring_calculado']
final_solo_rating_100=columnas_filtradas.drop_duplicates(subset = "contract_id_titular")

#UNIÓN TODOS LOS PARÁMETROS
join_final_completo_bonos=join_final_completo_bonos[['contract_id','rating_calculado','Dif_días']]
join_final_completo_bonos.columns=['contract_id','initial_rating_bonos','dif_días']
fichero_final=pd.merge(df_separado,join_final_completo_bonos,left_on='contract_id',right_on='contract_id',how='left')
filtrado_res=filtrado[['contract_id3','rating_calculado']]
filtrado_res.columns=['contract_id','initial_rating_no_bonos']
fichero_final_2=pd.merge(fichero_final,filtrado_res,left_on='contract_id',right_on='contract_id',how='left')
df_str_gbp_2=df_str_gbp[['contract_id','entityglobalrating']]
df_str_gbp_2.columns=['contract_id','initial_rating_gbp_hist']
fichero_final_3=pd.merge(fichero_final_2,df_str_gbp_2,left_on='contract_id',right_on='contract_id',how='left')
final_solo_rating_100=final_solo_rating_100[['contract_id_titular','initial_scoring_calculado']]
final_solo_rating_100.columns=['contract_id','initial_rating_av_100']
fichero_final_4=pd.merge(fichero_final_3,final_solo_rating_100,left_on='contract_id',right_on='contract_id',how='left')



#Convertir comas en puntos para float en gbp_hist y no bonos
fichero_final_5=fichero_final_4.fillna(9999)
try:
    fichero_final_5['initial_rating_gbp_hist'] = fichero_final_5['initial_rating_gbp_hist'].astype(str)
    fichero_final_5['initial_rating_no_bonos'] = fichero_final_5['initial_rating_no_bonos'].astype(str)
    fichero_final_5['initial_scoring_rating'] = fichero_final_5['initial_scoring_rating'].astype(str)
    
    fichero_final_5['initial_rating_gbp_hist']=fichero_final_5['initial_rating_gbp_hist'].str.replace(",", ".").astype(float)
    fichero_final_5['initial_rating_no_bonos']=fichero_final_5['initial_rating_no_bonos'].str.replace(",", ".").astype(float)
    fichero_final_5['initial_scoring_rating']=fichero_final_5['initial_scoring_rating'].str.replace(",", ".").astype(float)
except KeyError:
    print(".")

#ELECCIÓN INITIAL_RATING
i=0
lista_eleccion=[]
lista_comentario=[]
for i in range(len(fichero_final_5['contract_id'])):
    if  fichero_final_5['initial_rating_gbp_hist'][i]!=9999 and fichero_final_5['customer_segment_code'][i]=="08":
        lista_eleccion.append(fichero_final_5['initial_rating_gbp_hist'][i])
        lista_comentario.append('Rating gbp_hist')
    elif  fichero_final_5['initial_rating_bonos'][i]!=9999 and (fichero_final_5['product_grouper'][i]=="AA" or fichero_final_5['product_grouper'][i]=="RF") and fichero_final_5['tipo'][i]=="titular":
        lista_eleccion.append(fichero_final_5['initial_rating_bonos'][i])
        lista_comentario.append('Rating bonos')
    elif  fichero_final_5['initial_rating_no_bonos'][i]!=9999: #and (fichero_final_5['product_grouper'][i]!="AA" and fichero_final_5['product_grouper'][i]!="RF"):
        lista_eleccion.append(fichero_final_5['initial_rating_no_bonos'][i])
        lista_comentario.append('Rating no bonos')
    elif  fichero_final_5['initial_rating_av_100'][i]!=9999:
        lista_eleccion.append(fichero_final_5['initial_rating_av_100'][i])
        lista_comentario.append('Rating avalista 100%')    
    else:
        lista_eleccion.append(0.0)
        lista_comentario.append('No encontrado')
        




fichero_final_5.insert(16,'initial_rating_calculado',lista_eleccion,allow_duplicates=True)
fichero_final_5.insert(17,'Comentario_eleción',lista_comentario,allow_duplicates=True)

lista_ok=[]

fichero_final_5['initial_rating_calculado'] = fichero_final_5['initial_rating_calculado'].astype(str)
fichero_final_5['initial_rating_calculado']=fichero_final_5['initial_rating_calculado'].str.replace(",", ".").astype(float)

i=0
for i in range(len(fichero_final_5['contract_id'])):
    if float(fichero_final_5['initial_scoring_rating'][i])==float(fichero_final_5['initial_rating_calculado'][i]):
        lista_ok.append('OK rating')
    else:
        lista_ok.append('NO OK rating')

fichero_final_5.insert(18,'Coincide rating',lista_ok,allow_duplicates=True)


writer = pd.ExcelWriter(output, engine='xlsxwriter')

# #CÓDIGO PARA GENERAR EL EXCEL
filtrado.to_excel(writer,index=False,sheet_name="no_bonos_comp")
join_join_gbp_v1.to_excel(writer,index=False,sheet_name="gbp_inicio")
join_join_aqp_bonos_v2.to_excel(writer,index=False,sheet_name="bonos_bruto")
df_str_gbp.to_excel(writer,index=False,sheet_name="initial_gbp")
filtrado_res.to_excel(writer,index=False,sheet_name="no_bonos")
join_final_completo_bonos.to_excel(writer,index=False,sheet_name="bonos")
final_solo_rating_100.to_excel(writer,index=False,sheet_name="av_100")
df_input.to_excel(writer,index=False,sheet_name='input')
df_stratus.to_excel(writer,index=False,sheet_name='stratus')
df_gbp_hist.to_excel(writer,index=False,sheet_name='gbp_hist')
fichero_final_2.to_excel(writer,index=False,sheet_name='prueba')
fichero_final_5.to_excel(writer,index=False,sheet_name='prueba_fin')
filtrado.to_excel(writer,index=False,sheet_name='no_bonos_comp')




writer.close()
print("cerró el archivo")