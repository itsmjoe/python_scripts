
# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from process.monitoreo.repository.monitoreo import __get_cat_snmp_ips_red_corp_devices
from process.monitoreo.snmp.snmp_monitoreo import __save_data, __execute_snmpt_command_v2, __replace_values
from process.monitoreo.snmp import DIRECTORY_RED_CORP_DEVICES_DATA

#######################
#### CONFIGURATION ####
#######################

logger = logging.getLogger(__name__)

KPIS = {'CPU': {'kpi': 'CISCO-PROCESS-MIB::cpmCPUTotal5minRev', 'replace': ['Gauge32:', 'percent']},
        'Fan_status': {'kpi': 'CISCO-ENTITY-FRU-CONTROL-MIB::cefcFanTrayOperStatus', 'replace': ['INTEGER:']},
        'Memoria': [{'kpi': 'CISCO-PROCESS-MIB::cpmCPUMemoryUsed', 'replace': ['Gauge32:', 'kilo-bytes']},
                    {'kpi': 'CISCO-PROCESS-MIB::cpmCPUMemoryFree', 'replace': ['Gauge32:', 'kilo-bytes']}],
        'Pw_su': {'kpi': 'CISCO-ENTITY-FRU-CONTROL-MIB::cefcFRUPowerOperStatus', 'replace': ['INTEGER:']},
        'Sensores': [{'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorValue', 'replace': ['INTEGER:']},
                     {'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorThresholdRelation', 'replace': ['INTEGER:']},
                     {'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorThresholdValue', 'replace': ['INTEGER:']}],
        'Temperatura': [{'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorValue', 'replace': ['INTEGER:']},
                        {'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorThresholdRelation', 'replace': ['INTEGER:']},
                        {'kpi': 'CISCO-ENTITY-SENSOR-MIB::entSensorThresholdValue', 'replace': ['INTEGER:']}]}

IDS = ['Ip', 'Id', 'Node', 'Field']
OPERATOR = ['Value']
PARENTHESIS_MATCHES = ['Fan_status', 'Pw_su']

# ###########################################################
# #######  FUNCTIONS
# ###########################################################


def __convert_string_ids(df_data, ids):
    for field in ids:
        df_data[field] = df_data[field].astype(str)
        df_data.drop(df_data.loc[df_data[field] == 'No Such Object available on this agent at this OID'].index,
                     inplace=True)
    return df_data


def __get_data_value(value, field, kpi):
    return {'Id': value.split('=')[0].replace(kpi['kpi'] + '.', '').strip(),
            'Value': __replace_values(value.split('=')[1], kpi['replace']).strip(), 'Field': field,
            'Name': kpi['kpi'].split('::')[1]}


def __convert_to_dataframe(values, field, kpis):
    logger.info("###### __convert_to_dataframe ######")
    logger.info("field : {0}".format(field))
    data = {}
    count = 0
    for value in values:
        if value.strip('') != '':
            if type(kpis[field]) is list:
                kpi_list = kpis[field]
                for kpi in kpi_list:
                    if kpi['kpi'] in value:
                        data[count] = __get_data_value(value, field, kpi)
                        count += 1
            else:
                data[count] = __get_data_value(value, field, kpis[field])
                count += 1
    return pd.DataFrame(data).transpose()


def __get_kpis_snmpwalk(ip, kpis, comunidad):
    logger.info("###### __get_kpis_snmpwalk ######")
    logger.info("ip : {0}".format(ip))
    df_result = pd.DataFrame()
    for field in kpis:
        data = []
        if type(kpis[field]) is list:
            kpi_list = kpis[field]
            for kpi in kpi_list:
                data_item = __execute_snmpt_command_v2(comunidad, ip, kpi['kpi'])
                data += data_item
        else:
            data = __execute_snmpt_command_v2(comunidad, ip, kpis[field]['kpi'])
        df_data = __convert_to_dataframe(data, field, kpis)
        if not df_data.empty and df_result.empty:
            df_result = df_data
        elif not df_data.empty and not df_result.empty:
            df_result = pd.concat([df_data, df_result])
    df_result = df_result.fillna(0)

    return df_result


def __process_snmpwalk(ip, node, comunidad):
    logger.info("###### __process_snmpwalk ######")
    logger.info("ip : {0}".format(ip))
    logger.info("nodo : {0}".format(node))
    try:
        date_load = datetime.today()
        df_result = __get_kpis_snmpwalk(ip, KPIS, comunidad)
        df_result['Ip'] = ip
        df_result['Node'] = node
        df_result['Timestamp'] = date_load
        if not df_result.empty:
            df_result = __convert_string_ids(df_result, IDS)
            __save_data(ip, df_result, date_load, DIRECTORY_RED_CORP_DEVICES_DATA)
    except Exception as e:
        logger.error("Exception :", e)


def __proccess():
    logger.info("###### __process ######")
    logger.info('Creamos un pool  con 100 threads')
    df_ips = __get_cat_snmp_ips_red_corp_devices()
    executor = ThreadPoolExecutor(max_workers=100)
    for index, row in df_ips.iterrows():
        executor.submit(__process_snmpwalk, row['ip'], row['nodo'], row['comunidad'])


if __name__ == '__main__':
    __proccess()
