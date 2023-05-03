# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd

from process.monitoreo.repository.monitoreo import __get_cat_entidades_xag, __get_xag_error
from process.monitoreo.xag import PATH_SOURCE_1, PATH_SOURCE_2, PATH_FINAL, PATH_BACKUP_2, PATH_BACKUP_1
from process.monitoreo.utils.utils import __validate_columns_dataframe, __backup_original_files

###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)

COLUMNS_NAME = {0: 'serial_number', 1: 'message_type', 2: 'send_service_type', 3: 'receive_service_type',
                7: 'destination_address', 10: 'timestamp', 15: 'message_status', 20: 'err_code',
                30: 'charged_subscriber_type',
                31: 'charged_subscriber_number', 50: 'sm_sending_account', 51: 'sm_receiving_account'}

COLUMNS_XAG_SN_5M = ["timestamp", 'serial_number', "message_type", "send_service_type", "receive_service_type",
                     "destination_address", "message_status", "err_code", "charged_subscriber_type","charged_subscriber_number",
                     "sm_sending_account", "sm_receiving_account", "entidad","code_description"]

COLUMNS_XAG_TRANS_5M = ['timestamp', 'colector', 'message_type', 'sm_sending_account', 'sm_receiving_account',
                        'message_status', 'transacciones']

COLUMNS_XAG_ENTIDAD_5M = ['timestamp', 'message_type', 'entidad',
                          'message_status', 'transacciones']


###########################################################
#######  FUNCTIONS
###########################################################

def __change_message_type(df_data, df_cat_entidades):
    if not df_data.empty and not df_cat_entidades.empty:
        df_data_filter = df_data[
            df_data['sm_receiving_account'].isin(
                df_cat_entidades[df_cat_entidades['Tipo_proceso'] == 2]['SM_receiving_account'].values)].copy()
        df_data_filter['message_type'] = 62
        df_data_not_change = df_data[
            ~df_data['sm_receiving_account'].isin(
                df_cat_entidades[df_cat_entidades['Tipo_proceso'] == 2]['SM_receiving_account'].values)]

        return pd.concat([df_data_filter, df_data_not_change])


def __change_format_time(date):
    day = '{0:%Y-%m-%d %H}'.format(date)
    minute = int(int('{0:%M}'.format(date)) / 5) * 5
    return '{0}:{1}:00'.format(day, minute)


def __process_files(source_dir, collector, backup_dir):
    logger.info("###### __process_files ######")
    logger.info("source_dir :{0}".format(source_dir))
    logger.info("collector :{0}".format(collector))
    logger.info("backup_dir :{0}".format(backup_dir))
    list_files = glob.glob(source_dir)
    if len(list_files) > 0:
        df_cat_entidades = __get_cat_entidades_xag().rename({'Entidad': 'entidad'}, axis=1)
        df_xag_list_error = __get_xag_error()
        for file in list_files:
            logger.info("Read file :{0}".format(file))
            df_data = pd.read_csv(file, quoting=csv.QUOTE_NONE, header=None)
            df_data = df_data[[10, 0, 1, 2, 3, 7, 15, 20, 30, 31, 50, 51]].rename(COLUMNS_NAME, axis=1)
            df_data['colector'] = collector
            df_data['timestamp'] = pd.to_datetime(df_data['timestamp'])
            df_data['timestamp'] = df_data['timestamp'].apply(lambda x: __change_format_time(x))
            df_data = __change_message_type(df_data.copy(), df_cat_entidades)
            df_data['message_type'] = df_data['message_type'].apply(lambda x: 'MO' if x == 62 else 'MT')

            df_data['entidad'] = df_data['sm_sending_account'] + '_' + df_data['sm_receiving_account']
            df_result = df_data.merge(df_cat_entidades[['entidad']], on='entidad', how='inner')
            df_xag_list_error['message_status'] = df_xag_list_error['err_code'].astype(str)
            df_result['message_status'] = df_result['message_status'].astype(str)
            df_result = df_result.merge(df_xag_list_error[['message_status', 'code_description']], on='message_status',
                                        how='left')
            df_result['code_description'] = df_result.apply(lambda x: x['code_description'] if not pd.isnull(x['code_description']) else 'Error desconocido ({0})'.format(x['message_status']), axis=1)


            
            df_transacc_5m = df_result[
                ['timestamp', 'colector', 'message_type', 'sm_sending_account', 'sm_receiving_account',
                 'message_status', 'serial_number']].groupby(
                ['timestamp', 'colector', 'message_type', 'sm_sending_account', 'sm_receiving_account',
                 'message_status']).count().reset_index().rename({'serial_number': 'transacciones'}, axis=1)
            df_entidad_5m = df_result[['timestamp', 'message_type', 'entidad',
                                       'message_status', 'serial_number']].groupby(
                ['timestamp', 'message_type', 'entidad',
                 'message_status']).count().reset_index().rename({'serial_number': 'transacciones'}, axis=1)
            file_name = os.path.basename(file)

            df_result = __validate_columns_dataframe(df_result, COLUMNS_XAG_SN_5M)
            df_entidad_5m = __validate_columns_dataframe(df_entidad_5m, COLUMNS_XAG_ENTIDAD_5M)
            df_transacc_5m = __validate_columns_dataframe(df_transacc_5m, COLUMNS_XAG_TRANS_5M)
            logger.info("Create New file :{0}".format(
                PATH_FINAL + 'xag_sms_sn_5min_{0}.csv'.format(os.path.splitext(file_name)[0])))
            df_result.to_csv(PATH_FINAL + 'xag_sms_sn_5min_{0}.csv'.format(os.path.splitext(file_name)[0]), index=None,
                           header=False)
            logger.info("Create New file :{0}".format(
                PATH_FINAL + 'xag_sms_entidad_5min_{0}.csv'.format(os.path.splitext(file_name)[0])))
            df_entidad_5m.to_csv(PATH_FINAL + 'xag_sms_entidad_5min_{0}.csv'.format(os.path.splitext(file_name)[0]),
                                 index=None, header=False)
            logger.info("Create New file :{0}".format(
                PATH_FINAL + 'xag_sms_trans_5min_{0}.csv'.format(os.path.splitext(file_name)[0])))
            df_transacc_5m.to_csv(PATH_FINAL + 'xag_sms_trans_5min_{0}.csv'.format(os.path.splitext(file_name)[0]),
                                  index=None, header=False)
            logger.info("Delete file:{0}".format(file))
            __backup_original_files(file, backup_dir)


def main():
    __process_files(PATH_SOURCE_1, 1, PATH_BACKUP_1)
    __process_files(PATH_SOURCE_2, 2, PATH_BACKUP_2)


if __name__ == "__main__":
    main()

