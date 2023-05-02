# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd

from process.monitoreo.repository.monitoreo import __get_cat_ip_cisco_prime, __get_cat_interface_dash_cisco_prime
from process.monitoreo.ciscoprime import PATH_SOURCE, PATH_FINAL, PATH_TEMP, COLUMNS_DATA
from process.monitoreo.utils.utils import __validate_columns_dataframe

###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)
PIPELINE = 'cisco_prime'
FILE_NAME_DEVICES = 'DEVICES-'
FILE_NAME_DASHBOARD = 'DASHBOARD-'


###########################################################
#######  FUNCTIONS
###########################################################


def __process_files():
    logger.info("###### __process_files ######")
    list_files = glob.glob(PATH_SOURCE)
    if len(list_files) > 0:
        df_cat_ip = __get_cat_ip_cisco_prime()
        df_cat_int_dash = __get_cat_interface_dash_cisco_prime()

        for file in list_files:
            logger.info("Read file :{0}".format(file))
            df_data = pd.read_csv(file, quoting=csv.QUOTE_NONE)
            df_data['Interface'] = df_data['Interface'].astype(str)

            df_data_devices = df_data.merge(df_cat_ip[['IP', 'Estado', 'AlarmaTraf', 'AlarmaDisp']],
                                            left_on=['Node'],
                                            right_on=['IP'], how='inner')

            df_data_dashboard = df_data.merge(df_cat_int_dash[['IP', 'Interface', 'Cliente', 'Sucursal', 'Servicio']],
                                              left_on=['Node', 'Interface'],
                                              right_on=['IP', 'Interface'], how='inner')

            file_name = os.path.basename(file)
            logger.info("Create New file :{0}".format(PATH_FINAL + FILE_NAME_DEVICES + file_name))
            logger.info("Create New file :{0}".format(PATH_FINAL + FILE_NAME_DASHBOARD + file_name))

            df_data_devices = __validate_columns_dataframe(df_data_devices, COLUMNS_DATA)
            df_data_dashboard = __validate_columns_dataframe(df_data_dashboard, COLUMNS_DATA)

            df_data_devices.to_csv(PATH_FINAL + FILE_NAME_DEVICES + file_name,
                                   columns=['Timestamp', 'Node', 'Interface', 'SendTotalPkts', 'SendBytes',
                                            'SendBitRate',
                                            'SendErrors', 'SendDiscards', 'ReceiveTotalPkts', 'ReceiveBytes',
                                            'ReceiveBitRate', 'ReceiveErrors', 'ReceiveDiscards',
                                            'IfIndex'],
                                   index=None, header=False)
            logger.info("Delete file:{0}".format(file))

            df_data_dashboard.to_csv(PATH_FINAL + FILE_NAME_DASHBOARD + file_name,
                                     columns=['Timestamp', 'Node', 'Interface', 'SendTotalPkts', 'SendBytes',
                                              'SendBitRate',
                                              'SendErrors', 'SendDiscards', 'ReceiveTotalPkts', 'ReceiveBytes',
                                              'ReceiveBitRate', 'ReceiveErrors', 'ReceiveDiscards',
                                              'IfIndex'],
                                     index=None, header=False)
            logger.info("Delete file:{0}".format(file))

            df_temp = pd.read_csv(file, quoting=csv.QUOTE_NONE)
            df_temp = df_temp.where(pd.notnull(df_temp), "0.00")
            df_data_devices_filtro = df_temp.loc[~df_temp['Interface'].str.contains("Voice Over IP Peer|"
                                                                                    "Voice Encap|"
                                                                                    "Backplane|"
                                                                                    "x25-node|"
                                                                                    "ip-router|"
                                                                                    "Switch Port interface:|"
                                                                                    "NVI0|"
                                                                                    "HUAWEI; AR Series|"
                                                                                    "Foreign Exchang", case=False,
                                                                                    na=False)]
            df_data_devices_filtro = df_data_devices_filtro.merge(
                df_cat_ip[['IP', 'Estado', 'AlarmaTraf', 'AlarmaDisp']],
                left_on=['Node'],
                right_on=['IP'], how='inner')

            df_data_devices_filtro.to_csv(PATH_FINAL + 'filtro-' + file_name,
                                          columns=['Timestamp', 'Node', 'Interface', 'SendTotalPkts', 'SendBytes',
                                                   'SendBitRate',
                                                   'SendErrors', 'SendDiscards', 'ReceiveTotalPkts', 'ReceiveBytes',
                                                   'ReceiveBitRate', 'ReceiveErrors', 'ReceiveDiscards',
                                                   'IfIndex'],
                                          index=None, header=False)

            os.remove(file)


def main():
    __process_files()


if __name__ == "__main__":
    main()


