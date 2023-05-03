
# -*- coding: utf-8 -*-
import pandas as pd
import logging
import subprocess

from process.monitoreo.repository.monitoreo import __get_cat_entidades_service, __get_cat_snmp_ips
from process.monitoreo.snmp import SEC_NAME, SEC_LEVEL, AUTH_PASS, AUTH_PROTO, PRIVATE_PASS, PRIVATE_PROTO, \
    DIRECTORY_LAST_DATA, DIRECTORY_DATA
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime

#######################
#### CONFIGURATION ####
#######################

logger = logging.getLogger(__name__)
KPIS = {'Interface': {'kpi': 'IF-MIB::ifDescr', 'replace': ['STRING:']},
        'HighSpeed': {'kpi': 'IF-MIB::ifHighSpeed', 'replace': ['Gauge32:']},
        'AdminStatus': {'kpi': 'IF-MIB::ifAdminStatus', 'replace': ['INTEGER:']},
        'OperStatus': {'kpi': 'IF-MIB::ifOperStatus', 'replace': ['INTEGER:']},
        'InOctets': {'kpi': 'IF-MIB::ifHCInOctets', 'replace': ['Counter64:']},
        'OutOctets': {'kpi': 'IF-MIB::ifHCOutOctets', 'replace': ['Counter64:']},
        'InUcastPkts': {'kpi': 'IF-MIB::ifHCInUcastPkts', 'replace': ['Counter64:']},
        'OutUcastPkts': {'kpi': 'IF-MIB::ifHCOutUcastPkts', 'replace': ['Counter64:']},
        'InDiscards': {'kpi': 'IF-MIB::ifInDiscards', 'replace': ['Counter32:']},
        'OutDiscards': {'kpi': 'IF-MIB::ifOutDiscards', 'replace': ['Counter32:']},
        'InErrors': {'kpi': 'IF-MIB::ifInErrors', 'replace': ['Counter32:']},
        'OutErrors': {'kpi': 'IF-MIB::ifOutErrors', 'replace': ['Counter32:']}}

IDS = ['Ip', 'Interface', 'Id', 'Node']
OPERATOR = ['InOctets', 'OutOctets', 'InUcastPkts', 'OutUcastPkts', 'InDiscards', 'OutDiscards',
            'InErrors', 'OutErrors']

INTERVAL = 5


###########################################################
#######  FUNCTIONS
###########################################################


def __execute_snmpt_command_v3(sec_name, sec_level, private_proto, private_pass, auth_proto, auth_pass, ip, kpi):
    logger.info("###### __execute_snmpt_command ######")
    logger.info("sec_name : {0}".format(sec_name))
    logger.info("sec_level : {0}".format(sec_level))
    logger.info("private_proto : {0}".format(private_proto))
    logger.info("private_pass : {0}".format(private_pass))
    logger.info("auth_proto : {0}".format(auth_proto))
    logger.info("auth_pass : {0}".format(auth_pass))
    logger.info("ip : {0}".format(ip))
    logger.info("kpi : {0}".format(kpi))
    useless_cat_call = subprocess.run(
        ['/usr/bin/snmpwalk', '-v3', '-u', sec_name, '-l', sec_level, '-a', private_proto, '-A', private_pass, '-x',
         auth_proto, '-X', auth_pass, ip, kpi], stdout=subprocess.PIPE)
    return useless_cat_call.stdout.decode("utf-8").split('\n')


def __execute_snmpt_command_v2(comunidad, ip, kpi):
    logger.info("###### __execute_snmpt_command ######")
    logger.info("sec_name : {0}".format(comunidad))
    logger.info("ip : {0}".format(ip))
    logger.info("kpi : {0}".format(kpi))
    useless_cat_call = subprocess.run(
        ['/usr/bin/snmpwalk', '-v2c', '-c', comunidad, ip, kpi], stdout=subprocess.PIPE)
    return useless_cat_call.stdout.decode("utf-8").split('\n')


def __replace_values(value, replaces):
    for replace in replaces:
        value = value.replace(replace, '')
    return value


def __convert_string_ids(df_data, ids):
    for field in ids:
        df_data[field] = df_data[field].astype(str)
    return df_data


def __convert_to_dataframe(values, field, kpis):
    logger.info("###### __convert_to_dataframe ######")
    logger.info("field : {0}".format(field))
    data = {}
    count = 0
    for value in values:
        if value.strip('') != '':
            data[count] = {'Id': value.split('=')[0].replace(kpis[field]['kpi'] + '.', '').strip(),
                           field: __replace_values(value.split('=')[1], kpis[field]['replace']).strip()}
            count = count + 1
    return pd.DataFrame(data).transpose()


def __get_kpis_snmpwalk(ip, kpis, version, encriptacion, comunidad=''):
    logger.info("###### __get_kpis_snmpwalk ######")
    logger.info("ip : {0}".format(ip))
    df_result = pd.DataFrame()
    for field in kpis:
        data = []
        if 'v3' in version:
            data = __execute_snmpt_command_v3(SEC_NAME, SEC_LEVEL, PRIVATE_PROTO, PRIVATE_PASS, encriptacion, AUTH_PASS,
                                              ip, kpis[field]['kpi'])
        elif 'v2c' in version:
            data = __execute_snmpt_command_v2(comunidad, ip, kpis[field]['kpi'])
        df_data = __convert_to_dataframe(data, field, kpis)
        if not df_data.empty and df_result.empty:
            df_result = df_data
        elif not df_data.empty and not df_result.empty:
            df_result = df_result.merge(df_data, on='Id', how='outer')
    for operator in OPERATOR:
        if operator in df_result.columns:
            df_result[operator] = pd.to_numeric(df_result[operator])
    df_result = df_result.fillna(0)

    return df_result


def __read_last_data(ip):
    logger.info("###### __read_last_data ######")
    logger.info("ip : {0}".format(ip))
    logger.info("File : {0}".format(DIRECTORY_LAST_DATA + '{0}.csv'.format(ip.replace('.', '_'))))
    if os.path.isfile(DIRECTORY_LAST_DATA + '{0}.csv'.format(ip.replace('.', '_'))):
        return pd.read_csv(DIRECTORY_LAST_DATA + '{0}.csv'.format(ip.replace('.', '_')))
    else:
        return pd.DataFrame()


def __save_last_data(ip, df_data):
    logger.info("###### __save_last_data ######")
    logger.info("ip : {0}".format(ip))
    df_data.to_csv(DIRECTORY_LAST_DATA + '{0}.csv'.format(ip.replace('.', '_')), index=False)


def __save_data(ip, df_data, date, directory):
    logger.info("###### __save_data ######")
    logger.info("ip : {0}".format(ip))
    logger.info(
        "File : {0}".format(directory + '{0}_{1:%Y_%m_%d_%H_%M_%S}.csv'.format(ip.replace('.', '_'), date)))
    df_data.to_csv(directory + '{0}_{1:%Y_%m_%d_%H_%M_%S}.csv'.format(ip.replace('.', '_'), date), index=False, header=False)


def __cal_diff_by_interval(df_data, df_last_data):
    logger.info("###### __cal_diff_by_interval ######")
    logger.info("INTERVAL : {0}".format(INTERVAL))
    df_result = df_data.merge(df_last_data[IDS + OPERATOR + ['Timestamp']], on=IDS, how='outer')
    df_result['diff_time'] = (df_result['Timestamp_x'] - df_result['Timestamp_y']).astype('timedelta64[m]')
    df_result = df_result.drop(['Timestamp_x', 'Timestamp_y'], axis=1)
    for column in OPERATOR:
        df_result[column] = (df_result[column + '_x'] - df_result[column + '_y'])
        df_result = df_result.drop([column + '_x', column + '_y'], axis=1)

    return df_result


def __process_snmpwalk(ip, nodo, df_cat_entidades, version, encriptacion):
    logger.info("###### __process_snmpwalk ######")
    logger.info("ip : {0}".format(ip))
    logger.info("nodo : {0}".format(nodo))
    try:
        date_load = datetime.today()
        df_result = __get_kpis_snmpwalk(ip, KPIS, version, encriptacion)
        df_result['Ip'] = ip
        df_result['Node'] = nodo
        df_result['Timestamp'] = date_load
        logger.info(df_result)
        if not df_result.empty:
            df_result = __convert_string_ids(df_result, IDS)
            df_last_data = __read_last_data(ip)
            if not df_last_data.empty:
                df_last_data = __convert_string_ids(df_last_data, IDS)
                df_last_data['Timestamp'] = pd.to_datetime(df_last_data['Timestamp'])
                df_diff = __cal_diff_by_interval(df_result, df_last_data)
                df_diff['Timestamp'] = date_load
                df_cat_entidades['Interface'] = df_cat_entidades['interface'].str.strip()
                df_cat_entidades['Node'] = df_cat_entidades['device'].str.strip()
                df_diff = df_diff.merge(
                    df_cat_entidades[['id', 'Node', 'Interface', 'id_Servicio', 'agrupacion', 'etiqueta']],
                    on=['Node', 'Interface'], how='left')
                __save_data(ip, df_diff, date_load, DIRECTORY_DATA)
            __save_last_data(ip, df_result)
    except Exception as e:
        logger.error("Exception :", e)


def __process():
    logger.info("###### __process ######")
    logger.info('Creamos un pool  con 30 threads')
    df_cat_entidades = __get_cat_entidades_service()
    df_ips = __get_cat_snmp_ips()
    executor = ThreadPoolExecutor(max_workers=100)
    for index, row in df_ips.iterrows():
        executor.submit(__process_snmpwalk, row['ip'], row['nodo'], df_cat_entidades.copy(), row['version'],
                        row['encriptacion'])


if __name__ == '__main__':
    __process()