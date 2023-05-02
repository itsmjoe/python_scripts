# -*- coding: utf-8 -*-
import glob
import logging
import os
from shutil import move
import re

#############################################
#######  CONFIGURATION
#############################################
logger = logging.getLogger(__name__)


#############################################
#######  FUNCTION
#############################################

def __get_epoch_time(date_str, format_date="%Y-%m-%d %H:%M:%S"):
    date_time = __to_date(date_str, format_date)
    return int(date_time.strftime("%s"))


def __get_epoch_time_ms(date_str, format_date="%Y-%m-%d %H:%M:%S"):
    date_time = __to_date(date_str, format_date)
    return int(date_time.strftime("%s")) * 1000


def __to_date(date_str, format_date="%Y-%m-%d %H:%M:%S"):
    from datetime import datetime
    if not date_str:
        return datetime.today().date()
    return datetime.strptime(date_str, format_date)


def __get_files(path):
    logger.info("####  get_files ####")
    logger.info("path : {0}".format(path))
    file_list = [f for f in glob.glob(path + "*.csv", recursive=True)]
    for f in file_list:
        logger.info(f)
    return file_list


def __backup_original_files(file, backup_path):
    logger.info("####  __backup_original_files ####")
    logger.info("file : {0}".format(file))
    __create_directory(backup_path)
    file_name = os.path.basename(file)
    move(file, "{0}{1}".format(backup_path, file_name))


def __backup_original_files_error(file, backup_path):
    logger.info("####  __backup_original_files_error ####")
    logger.info("file : {0}".format(file))
    __create_directory(backup_path)
    file_name = os.path.basename(file)
    move(file, "{0}{1}.Error".format(backup_path, file_name))


def __create_directory(directory):
    logger.info("# Creating the directory: {0}".format(directory))
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        logger.info("# The directory exists : {0}".format(directory))


def __validate_columns_dataframe(df_data, columns):
    logger.info("####  __validate_columns_dataframe ####")
    logger.info("df_data : {0}".format(len(df_data)))
    logger.info("columns : {0}".format(columns))

    if not df_data.empty:
        for column in columns:
            if column not in df_data.columns:
                df_data[column] = None
        return df_data[columns]
    else:
        return df_data
