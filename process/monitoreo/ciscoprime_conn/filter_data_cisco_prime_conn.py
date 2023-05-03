# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd
import time

from process.monitoreo.repository.monitoreo import __get_cat_ip_cisco_prime
from process.monitoreo.ciscoprime_conn import PATH_SOURCE, PATH_FINAL
from datetime import datetime, date, time, timedelta

###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)



###########################################################
#######  FUNCTIONS
###########################################################


def __process_files():
    logger.info("###### __process_files ######")
    list_files = glob.glob(PATH_SOURCE)
    if len(list_files) > 0:
        df_cat_ip = __get_cat_ip_cisco_prime()
        for file in list_files:
            logger.info("Read file :{0}".format(file))
            df_data = pd.read_csv(file, quoting=csv.QUOTE_NONE, sep='\t',header=None,names=['Timestamp','IP','Disponibilidad','Intentos','Exitos','Latencia','Latencia_Max','Latencia_Min'])
            df_data = df_data.merge(df_cat_ip[['IP']],how='inner')
            df_right_data = df_data.copy().drop_duplicates(['IP'], keep="first")
            df_right_data = df_right_data.merge(df_cat_ip[['IP']], how='right')
            nan_values = df_right_data[df_right_data.isna().any(axis=1)]
            tm = datetime.now()
            tm = tm - timedelta(minutes=tm.minute % 5)
            df_right_data = nan_values.assign(Timestamp = tm.strftime("%Y-%m-%d-%H-%M"), Disponibilidad=0, Intentos=10, Exitos=0, Latencia=0, Latencia_Max=0, Latencia_Min=0)
            df_data = df_data.append(df_right_data, ignore_index = True)
            file_name = os.path.basename(file)
            logger.info("Create New file :{0}".format(PATH_FINAL + file_name))
            df_data.to_csv(PATH_FINAL + file_name, index=None, header=False,sep='\t')
            logger.info("Delete file:{0}".format(file))
            os.remove(file)


def main():
    __process_files()


if __name__ == "__main__":
    main()

