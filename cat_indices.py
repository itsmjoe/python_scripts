from elasticsearch import Elasticsearch
from pandasticsearch import Select
from datetime import datetime
from dateutil import parser
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import logging
import numpy as np
import json
from pandas.io.json import json_normalize
import pytz
import mysql.connector

logger = logging.getLogger(__name__)

print('###INICIO DEL PROCESO###')

POSTGRESQL_IP = "10.225.1.1"
POSTGRESQL_PORT = 5432
POSTGRESQL_DATABASE = "general"
POSTGRESQL_USER = "xxxx"
POSTGRESQL_PASS = "xxxx"
DB_CONFIGURATION_USER = "app_mos"
DB_CONFIGURATION_PASS = "xxxx"
DB_CONFIGURATION_IP = "192.168.50.22"
DB_CONFIGURATION_PORT = 3306
DB_CONFIGURATION_DB = "xxxx"


SQL_CATALOGO_INDICES_DELETE_BY_ID = "DELETE FROM catalogo_indices WHERE id = %s"

def __get_engine_configuration():
    logger.info("###### __get_connection_configuration ######")
    return create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}'.format(user=DB_CONFIGURATION_USER,
                                                                                 password=DB_CONFIGURATION_PASS,
                                                                                 host=DB_CONFIGURATION_IP,
                                                                                 port=DB_CONFIGURATION_PORT,
                                                                                 schema=DB_CONFIGURATION_DB),
        echo=False)


def __get_connection_configuration():
    logger.info("###### __get_connection_configuration ######")

    return mysql.connector.connect(host=DB_CONFIGURATION_IP, port=DB_CONFIGURATION_PORT,
                                   user=DB_CONFIGURATION_USER,
                                   database=DB_CONFIGURATION_DB,
                                   password=DB_CONFIGURATION_PASS)


def __delete_statement(statement, part_id):
    logger.info("### __delete_from_snmp_net_members_by_node ###")
    logger.info("Part Id: " + str(part_id))
    conn = __get_connection_configuration()
    cur = conn.cursor()
    try:
        cur.execute(statement, (part_id,))
        rows_deleted = cur.rowcount
        conn.commit()
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        cur.close()
        conn.close()
    return rows_deleted


def __save_dataframe_in_table(dataframe, table):
    logger.info("### __save_dataframe ###")
    conn = __get_engine_configuration()
    try:
        dataframe.to_sql(table, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.error("Exception :", e)
        raise e


def __get_eliminados(indicesPD,indices_existentesPD):
    df_1_list = list(indicesPD['nombre_indice'])
    data = indices_existentesPD[~(indices_existentesPD['nombre_indice'].isin(df_1_list))]
    return data

def eliminar_indices(indicesPD,indices_existentesPD):
    data = __get_eliminados(indicesPD, indices_existentesPD)
    id_del = data['id'].unique()
    for i in id_del:
        __delete_statement(SQL_CATALOGO_INDICES_DELETE_BY_ID, int(i))
    print(data)

def __get_index(cluster):
    IP_ELASTIC= cluster ### Cluster01 10.225.245.238
    PORT_ELASTIC=9200
    # es = Elasticsearch(hosts=[{'host': IP_ELASTIC, 'port': PORT_ELASTIC}])
    es = Elasticsearch("http://{0}:{1}".format(IP_ELASTIC, PORT_ELASTIC))
    indice = es.indices.get_alias().keys()
    indice_sort = sorted(indice)
    data = pd.DataFrame(list(indice_sort), columns=['nombre'])
    return data

def __get_alias(data):
    index = list(data['nombre'])
    encontrado = []
    for cadena in index:
        encontrado.append(cadena.find('20'))
    data['encontrado'] = encontrado
    coincidencia = zip(list(data['encontrado']),list(data['nombre']))
    alias = []
    for c in coincidencia:
        if c[0] == -1:
            alias.append(c[1])
        else:
            alias.append(c[1][:c[0]-1]+'*')
    data['nombre_indice'] = alias
    return data

def __get_elementos(ip):
    indice = __get_index(ip)
    indice_alias = __get_alias(indice)
    indice_alias = indice_alias.drop_duplicates(subset='nombre_indice')
    indice_alias = indice_alias.drop(['encontrado','nombre'],axis=1)
    return indice_alias

def __get_indices_existentes():
    datos = []
    cnn = __get_connection_configuration()
    cur = cnn.cursor()
    cur.execute("select id,nombre_indice,existe_cluster from catalogo_indices")
    rows = cur.fetchall()
    for rw in rows:
        datos.append([rw[0], rw[1], rw[2]])

    cur.close()
    cnn.close()
    return datos

indices = __get_elementos('192.168.50.28')

# for c in cluster_list:
    # indices.append(__get_elementos(int(c[0]),c[1]))

print(indices)

indices_existentes = __get_indices_existentes()

indices_existentesPD = pd.DataFrame(indices_existentes, columns=['id', 'nombre_indice', 'existe_cluster'])

eliminar = eliminar_indices(indices,indices_existentesPD)

id_ = sorted(list(indices_existentesPD['id']))

indices_nuevos = pd.merge(indices,indices_existentesPD,how='left',on=['nombre_indice'])

indices_nuevos = indices_nuevos.loc[indices_nuevos['existe_cluster'].isnull()]

active_status = 1
cluster_exists = 1
new_id = []
estado = []
n_id = id_[-1]
for l in range(len(indices_nuevos)):
    n_id += 1
    new_id.append(n_id)
    estado.append(active_status)

indices_nuevos['id'] = new_id
indices_nuevos['activo'] = estado

indices_nuevos = indices_nuevos[['id', 'nombre_indice', 'existe_cluster', 'activo']]
indices_nuevos['existe_cluster'] = cluster_exists

__save_dataframe_in_table(indices_nuevos, 'catalogo_indices')

# for m in indices_nuevos_list:
#     updateTable(m)


print('###FINALIZA PROCESO###')