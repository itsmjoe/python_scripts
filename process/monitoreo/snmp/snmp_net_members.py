
# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from process.monitoreo.repository.monitoreo import __get_cat_snmp_ips, __save_dataframe_snmp_net_members
from process.monitoreo.snmp.snmp_monitoreo import __get_kpis_snmpwalk, __save_data
from process.monitoreo.snmp import COMUNIDAD

#######################
#### CONFIGURATION ####
#######################

logger = logging.getLogger(__name__)
KPIS = {'Vecino': {'kpi': 'SNMPv2-SMI::enterprises.9.9.23.1.2.1.1.6', 'replace': ['STRING:']},
        'Interface': {'kpi': 'SNMPv2-SMI::enterprises.9.9.23.1.2.1.1.7', 'replace': ['STRING:']},
        'Serie': {'kpi': 'SNMPv2-SMI::enterprises.9.9.23.1.2.1.1.8', 'replace': ['STRING:']},
        'Serie_Part': {'kpi': 'SNMPv2-SMI::enterprises.9.9.23.1.2.1.1.9', 'replace': ['STRING:']}}

IDS = ['Ip', 'Id', 'Node', 'Vecino', 'Interface', 'Serie', 'Serie_Part']

###########################################################
#######  FUNCTIONS
###########################################################


def __convert_string_ids(df_data, ids):
    for field in ids:
        df_data[field] = df_data[field].astype(str)
        df_data[field] = df_data[field].str.replace('"', '')
        df_data.drop(df_data.loc[df_data[field] == 'No Such Object available on this agent at this OID'].index,
                     inplace=True)
        if 'Vecino' in field:
            df_data[field] = df_data[field].apply(lambda x: x.split('.')[0])
            df_data[field] = df_data[field].apply(lambda x: x.split('(')[0])
    return df_data


def __process_snmpwalk(ip, node, version, encriptacion):
    logger.info("###### __process_snmpwalk ######")
    logger.info("ip : {0}".format(ip))
    logger.info("nodo : {0}".format(node))
    try:
        date_load = datetime.today()
        df_result = __get_kpis_snmpwalk(ip, KPIS, version, encriptacion, COMUNIDAD)
        df_result['Ip'] = ip
        df_result['Node'] = node
        df_result['Timestamp'] = date_load
        if not df_result.empty:
            df_result = __convert_string_ids(df_result, IDS)
            df_result['Serie'] = df_result['Serie'] + df_result['Serie_Part']
            df_result.drop('Serie_Part', axis=1, inplace=True)
            __save_dataframe_snmp_net_members(df_result, node)
    except Exception as e:
        logger.error("Exception :", e)


def __proccess():
    logger.info("###### __process ######")
    logger.info('Creamos un pool  con 100 threads')
    df_ips = __get_cat_snmp_ips()
    executor = ThreadPoolExecutor(max_workers=100)
    for index, row in df_ips.iterrows():
        executor.submit(__process_snmpwalk, row['ip'], row['nodo'], row['version'], row['encriptacion'])


if __name__ == '__main__':
    __proccess()
