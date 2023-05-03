# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd

from process.monitoreo.extraccion.extraccion_interface import __filter_and_extract_interface
from process.monitoreo.repository.monitoreo import __get_cat_interfaces_cisco_core, __get_cat_extraccion_interface, \
    __get_cat_interface_dash_cisco_prime, __get_cat_sucursal_cisco_core
from process.monitoreo.ciscocore import PATH_SOURCE, PATH_FINAL, COLUMNS_DATA
from process.monitoreo.ciscoprime import PATH_FINAL_CISCO_PRIME, COLUMNS_DATA_CISCO_PRIME
from process.monitoreo.utils.utils import __validate_columns_dataframe

###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)
PIPELINE = 'cisco_core'
FILE_NAME_DASHBOARD = 'DASHBOARD-CC-'
FILE_NAME_DISP = 'DISPONIBILIDAD-'

###########################################################
#######  FUNCTIONS
###########################################################


def __process_files():
    logger.info("###### __process_files ######")
    list_files = glob.glob(PATH_SOURCE)
    if len(list_files) > 0:
        df_cat_interfaces = __get_cat_interfaces_cisco_core()
        df_cat_extraction = __get_cat_extraccion_interface(PIPELINE)
        df_cat_int_dash = __get_cat_interface_dash_cisco_prime()
        df_sucursales = __get_cat_sucursal_cisco_core()
        df_sucursales = df_sucursales.rename({"LocID": "loc_id"}, axis=1)
        for file in list_files:
            logger.info("Read file :{0}".format(file))
            df_data = pd.read_csv(file, quoting=csv.QUOTE_NONE)
            df_data['Interface'] = df_data['Interface'].astype(str)
            df_data = __filter_and_extract_interface(df_cat_extraction, df_data)
            if not df_data.empty:
                df_data_interfaces = df_data.merge(df_cat_interfaces[['id', 'device', 'interface', 'loc_id']],
                                        left_on=['Node', 'Interface'],
                                        right_on=['device', 'interface'], how='inner')
    
                df_data_dashboard = df_data.merge(df_cat_interfaces[['id', 'device', 'interface', 'loc_id']],
                                                  left_on=['Node', 'Interface'],
                                                  right_on=['device', 'interface'], how='inner')
                df_data_dashboard.drop(['Node', 'Interface'], axis=1, inplace=True)
                df_data_dashboard = df_data_dashboard.merge(df_cat_int_dash[['IP']],
                                                            left_on=['loc_id'],
                                                            right_on='IP', how='inner')
                df_data_dashboard['Node'] = df_data_dashboard['IP']
                df_data_dashboard['Interface'] = df_data_dashboard['IP']
                ##########################PARA DISPONIBILIDAD############################
                df_data_disp = df_data.merge(df_cat_interfaces[['id', 'device', 'interface', 'loc_id']],
                                             left_on=['Node', 'Interface'],
                                             right_on=['device', 'interface'], how='inner')
                if not df_data_disp.empty:
                    timestamp = df_data_disp['Timestamp'].unique()[0]
                    df_data_disp = df_data_disp.merge(df_sucursales[['loc_id', 'Status']], on='loc_id', how='right')
                    df_data_disp = df_data_disp[df_data_disp["Status"] == "ACT"]
                    df_data_disp = df_data_disp.fillna(0)
                    df_data_disp = df_data_disp[
                        ['Timestamp', 'loc_id', 'SendBytes', 'ReceiveBytes']].groupby(
                        ['loc_id', 'Timestamp']).sum().reset_index()
    
                    df_data_disp = df_data_disp[
                        (df_data_disp['SendBytes'] == 0) & (df_data_disp['ReceiveBytes'] == 0)]
                    df_data_disp['Disponibilidad'] = 0
                    df_data_disp['Timestamp'] = timestamp
    
                file_name = os.path.basename(file)
                logger.info("Create New file :{0}".format(PATH_FINAL + file_name))
                logger.info("Create New file :{0}".format(PATH_FINAL_CISCO_PRIME + FILE_NAME_DASHBOARD + file_name))
                df_data_interfaces = __validate_columns_dataframe(df_data_interfaces, COLUMNS_DATA)
                df_data_dashboard = __validate_columns_dataframe(df_data_dashboard, COLUMNS_DATA_CISCO_PRIME)
                df_data_interfaces.to_csv(PATH_FINAL + file_name, index=None, header=False)
                df_data_dashboard.to_csv(PATH_FINAL_CISCO_PRIME + FILE_NAME_DASHBOARD + file_name, index=None, header=False)
                df_data_disp.to_csv(PATH_FINAL + FILE_NAME_DISP + file_name, index=None, header=False)
            logger.info("Delete file:{0}".format(file))
            os.remove(file)

def main():
    __process_files()


if __name__ == "__main__":
    main()

