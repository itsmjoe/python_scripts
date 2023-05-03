# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd

from process.monitoreo.extraccion.extraccion_interface import __filter_and_extract_interface
from process.monitoreo.repository.monitoreo import __get_cat_entidades_service, __get_cat_extraccion_interface
from process.monitoreo.services import PATH_SOURCE, PATH_FINAL, COLUMNS_DATA
from process.monitoreo.utils.utils import __validate_columns_dataframe

###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)
PIPELINE = 'service'


###########################################################
#######  FUNCTIONS
###########################################################


def __process_files():
    logger.info("###### __process_files ######")
    list_files = glob.glob(PATH_SOURCE)
    if len(list_files) > 0:
        df_cat_entidades = __get_cat_entidades_service()
        df_cat_extraction = __get_cat_extraccion_interface(PIPELINE)
        logger.info("df_cat_extraction :{0}".format(len(df_cat_extraction)))
        for file in list_files:
            logger.info("Read file :{0}".format(file))
            df_data = pd.read_csv(file, quoting=csv.QUOTE_NONE)
            df_data['Interface'] = df_data['Interface'].astype(str)
            df_data = __filter_and_extract_interface(df_cat_extraction, df_data)
            df_cat_entidades['interface'] = df_cat_entidades['interface'].str.strip()
            df_cat_entidades['device'] = df_cat_entidades['device'].str.strip()
            df_data = df_data.merge(df_cat_entidades[['id', 'device', 'interface','id_Servicio', 'agrupacion','etiqueta']],
                                    left_on=['Node', 'Interface'],
                                    right_on=['device', 'interface'], how='inner')

            file_name = os.path.basename(file)
            logger.info("Create New file :{0}".format(PATH_FINAL + file_name))
            df_data = __validate_columns_dataframe(df_data, COLUMNS_DATA)
            df_data.to_csv(PATH_FINAL + file_name, index=None, header=False)
            logger.info("Delete file:{0}".format(file))
            os.remove(file)


def main():
    __process_files()


if __name__ == "__main__":
    main()
