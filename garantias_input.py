# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 17:43:13 2022

@author: x947159
"""
# import sqlite3
import pandas as pd
# import numpy as np
from sqlalchemy import create_engine

# from sqlalchemy.dialects.postgresql import insert
# from sqlalchemy import table, column
# from datetime import datetime, timedelta


###############################################################################

# NOMBRE DEL ARCHIVO QUE SE QUIERE SEPARAR (input)
file = 'input_motor_v3.xlsx'

# NOMBRE DEL ARCHIVO QUE SERÁ RESULTADO DEL CÓDIGO
output = 'input_separado_garantias.xlsx'

engine = create_engine('sqlite://', echo=False)
df = pd.read_excel(file, sheet_name='Sheet')

print("ARCHIVOS EXCEL LEIDOS")

lista = []
lista_2 = []
lista_11 = []
lista_12 = []
lista_13 = []
lista_14 = []
lista_15 = []
lista_16 = []
lista_17 = []
lista_18 = []
lista_19 = []
lista_21 = []
lista_22 = []
lista_23 = []
lista_24 = []
lista_25 = []
lista_ast=[]

for i in range(len(df['initial_scoring_rating'])):
    lista_ast.append('#')

df.insert(0,'ast',lista_ast,allow_duplicates=True)
df['cobertura_pers'] = df['ast'] + df['current_guarantee_coverage_percentage']
df['cobertura_real'] = df['ast'] + df['current_coverage_percentage']

# PARTE DONDE SE DIVIDEN LOS CAMPOS QUE TIENEN SEPARACIÓN "#"

for i in range(len(df['initial_scoring_rating'])):
    isr = df['initial_scoring_rating'].iloc[i]
    k = df['kgl4'].iloc[i]
    odr = str(df['opening_date_real'].iloc[i])
    pg = str(df['product_grouper'].iloc[i])
    ci = str(df['contract_id'].iloc[i])
    csc = str(df['customer_segment_code'].iloc[i])
    sr = str(df['scoring_rating'].iloc[i])
    cid = str(df['customer_id'].iloc[i])
    pnt = str(df['parent'].iloc[i])
    cob_pers = str(df['cobertura_pers'].iloc[i])
    cob_real = str(df['cobertura_real'].iloc[i])
    '''g_p=str(df['current_guarantee_nominal_coverage'].iloc[i])
    pg_p=str(df['current_guarantee_coverage_percentage'].iloc[i])
    g_r=str(df['current_nominal_coverage'].iloc[i])
    pg_r=str(df['current_coverage_percentage'].iloc[i])'''
    cargabal = str(df['cargabal_identification_code'].iloc[i])
    subsegment = str(df['customer_subsegment_code'].iloc[i])
    flag = str(df['public_administration_flag'].iloc[i])
    isr_2 = isr.split("#")
    k_2 = k.split("#")
    csc_2 = csc.split('#')
    sr_2 = sr.split('#')
    cid_2 = cid.split('#')
    pnt_2 = pnt.split('#')
    cargabal_2 = cargabal.split('#')
    subsegment_2 = subsegment.split('#')
    #g_p=g_p.split('#')
    cob_pers_2 = cob_pers.split('#')
    cob_real_2 = cob_real.split('#')

    # CADA CAMPO SE METE EN UNA LISTA
    lista.append(isr_2)
    lista_2.append(ci)
    lista_11.append(k_2)
    lista_12.append(odr)
    lista_13.append(pg)
    lista_14.append(csc_2)
    lista_15.append(sr_2)
    lista_16.append(cid_2)
    lista_17.append(pnt_2)
    lista_18.append(cargabal_2)
    lista_19.append(flag)
    lista_21.append(subsegment_2)
    lista_22.append(cob_pers_2)
    lista_23.append(cob_real_2)

print("CAMPOS SEPARADOS")

# CADA LISTA SE CONVIERTE EN UN DATAFRAME
kgl4_separado = pd.DataFrame(lista_11)
segment_code_separado = pd.DataFrame(lista_14)
scoring_rating_separado = pd.DataFrame(lista_15)
customer_id_separado = pd.DataFrame(lista_16)
parent_separado = pd.DataFrame(lista_17)
cargabal_separado = pd.DataFrame(lista_18)
subsegment_separado = pd.DataFrame(lista_21)
cob_pers_separado=pd.DataFrame(lista_22)
cob_real_separado=pd.DataFrame(lista_23)


filtrado = pd.DataFrame(lista)
filtrado.insert(1, 'c_id', lista_2, allow_duplicates=True)
filtrado.insert(2, 'op_date', lista_12, allow_duplicates=True)
filtrado.insert(3, 'p_grouper', lista_13, allow_duplicates=True)
filtrado.insert(4, 'flag', lista_19, allow_duplicates=True)

lista_3 = []
lista_4 = []
lista_5 = []
lista_6 = []
lista_7 = []
lista_8 = []
lista_9 = []
lista_10 = []
lista_20 = []
lista_30 = []
lista_40 = []
lista_50 = []
lista_60 = []
lista_70 = []
lista_80=[]
lista_90=[]

n_columnas = len(filtrado.columns)

i = 0
u = 1

# FILAS DE TITULARES Y AVALISTAS SE POSICIONAN UNA DEBAJO DE OTRA
while i < (len(filtrado[0])):
    if filtrado[u][i] != None:
        lista_3.append(filtrado[u][i])
        lista_4.append(filtrado['c_id'][i])
        lista_7.append(kgl4_separado[u][i])
        lista_8.append(filtrado['op_date'][i])
        lista_9.append(filtrado['p_grouper'][i])
        lista_70.append(filtrado['flag'][i])
        lista_10.append(segment_code_separado[u][i])
        lista_20.append(scoring_rating_separado[u][i])
        lista_30.append(customer_id_separado[u][i])
        lista_40.append(parent_separado[u][i])
        lista_50.append(cargabal_separado[u][i])
        lista_60.append(subsegment_separado[u][i])
        lista_80.append(cob_pers_separado[u][i])
        lista_90.append(cob_real_separado[u][i])

        if u == 1:
            lista_5.append('titular')
        else:
            lista_5.append('avalista' + str(((u) - 1)))
    i = i + 1
    if i == (len(filtrado[u])) and u <= n_columnas - 6:
        i = 0
        u = u + 1

print("FILAS POSICIONADAS UNA DEBAJO DE OTRA")

# AGRUPACIÓN DE COLUMNAS
filtrado2 = pd.DataFrame(lista_3)
filtrado2.columns = ['initial_scoring_rating']
filtrado2.insert(0, 'contract_id', lista_4, allow_duplicates=True)
filtrado2.insert(2, 'tipo', lista_5, allow_duplicates=True)
filtrado2.insert(3, 'kgl4', lista_7, allow_duplicates=True)
filtrado2.insert(4, 'opening_date_real', lista_8, allow_duplicates=True)
filtrado2.insert(5, 'product_grouper', lista_9, allow_duplicates=True)
filtrado2.insert(6, 'public_administration_flag', lista_70, allow_duplicates=True)
filtrado2.insert(7, 'customer_segment_code', lista_10, allow_duplicates=True)
filtrado2.insert(8, 'scoring_rating', lista_20, allow_duplicates=True)
filtrado2.insert(9, 'customer_id', lista_30, allow_duplicates=True)
filtrado2.insert(10, 'parent', lista_40, allow_duplicates=True)
filtrado2.insert(11, 'cargabal', lista_50, allow_duplicates=True)
filtrado2.insert(12, 'customer_subsegment_code', lista_60, allow_duplicates=True)
filtrado2.insert(13, 'cobertura_personal', lista_80, allow_duplicates=True)
filtrado2.insert(14, 'cobertura_real', lista_90, allow_duplicates=True)
filtrado2['contract_id2'] = filtrado2.contract_id.str.cat(filtrado2.tipo)
col_contract_id2 = filtrado2.pop('contract_id2')
filtrado2.insert(1, 'contract_id2', col_contract_id2, allow_duplicates=True)

# CÓDIGO PARA GENERAR EL EXCEL
filtrado2.to_excel(output, index=False)

print("ARCHIVO EXCEL GENERADO")
