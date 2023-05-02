# -*- coding: utf-8 -*-
import glob
import logging
import os
import csv
import pandas as pd

###########################################################
#######  COFIGURATION
###########################################################
logger = logging.getLogger(__name__)


REPLACE_WORDS = ['Giga', 'xe', 'ae', 'Ethe', 'BV', 'Port', 'ge', 'Vlan', 'TenGig', 'Hundre', 'FastE', 'Tunnel', 'port',
                 'Serial', 'BDI', {'search':'CHG000000123536-xe','replace': 'xe'}, {'search':'SV-PEND-xe', 'replace':'xe'}, {'search':'SV-PEND-Ten', 'replace':'Ten'}, 'Bundle',
                 'Hundred', 'Eth',
                 {'search':'.datos.temm', 'replace':''}]


###########################################################
#######  FUNCTIONS
###########################################################


def __clean_interface_data(interface):
    if interface:
        for word in REPLACE_WORDS:
            if type(word) is dict:
                interface = interface.replace(word['search'], word['replace'])
            else:
                interface = interface.replace('-{0}'.format(word), '|{0}'.format(word))

    return interface


def __get_interface_last_pipe(interface):
    if interface:
        return __clean_interface_data(interface).replace('_', '|').split('|')[-1]
    return interface


def __get_interface_service_semicolon(interface):
    if interface:
        temp_interface = interface.split(";")[0]
        temp_interface = temp_interface.split('|')[-1]
        return temp_interface.split('-')[-1]
    return interface


def __get_interface_service_underscore(interface):
    if interface:
        temp_interface = interface.split('_')
        if len(temp_interface) > 3:
            return temp_interface[3]
        else:
            return temp_interface[-1]
    return interface


def __get_interface_na(interface):
    return interface


def __get_contains_by_function_and_column(df_data, function, column):
    if not df_data.empty:
        df_filter = df_data[(df_data['nombre_columna'] == column) & (df_data['tipo_extraccion'] == function)]
        if not df_filter.empty:
            return '|'.join(df_filter['filtro'].values)
    return None


FUNCTION_EXTRACTION = {'LAST_PIPE': __get_interface_last_pipe,
                       'SERVICE_SEMICOLON': __get_interface_service_semicolon,
                       'SERVICE_UNDERSCORE': __get_interface_service_underscore,
                       'N/A': __get_interface_na}


def __filter_and_extract_interface(df_cat_extraction, df_data):
    logger.info("###### __filter_and_extract_interface ######")
    logger.info("Total Datos :{0}".format(len(df_data)))
    df_result = pd.DataFrame()
    for function in df_cat_extraction['tipo_extraccion'].unique():
        for column in df_cat_extraction['nombre_columna'][(df_cat_extraction['tipo_extraccion'] == function)].unique():
            filter_column = __get_contains_by_function_and_column(df_cat_extraction, function, column)
            logger.info("Filtro :{0}".format(filter_column))
            logger.info("columna :{0}".format(column))
            logger.info("Funcion :{0}".format(function))
            df_filter = df_data[df_data[column].str.contains(filter_column)].copy()
            df_filter['Node'] = df_filter['Node'].apply(
                lambda x: __clean_interface_data(x))
            df_filter['Interface'] = df_filter['Interface'].apply(
                lambda x: FUNCTION_EXTRACTION[function](x))
            df_result = pd.concat([df_result, df_filter])
    df_result = df_result.drop_duplicates()
    logger.info("Total Filtro :{0}".format(len(df_result)))
    return df_result
