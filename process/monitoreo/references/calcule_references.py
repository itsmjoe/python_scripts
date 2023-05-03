# -*- coding: utf-8 -*-

import logging
import pandas as pd
import pytz
from elasticsearch import Elasticsearch
from dateutil import tz
from elasticsearch.helpers import bulk
from datetime import datetime, time
from datetime import timedelta
import math
import click
import pytz

###########################################################
#######  COFIGURATION
###########################################################
from process import CONFIGURATION
from process.monitoreo.utils import utils

logger = logging.getLogger(__name__)

IPS_CLUSTER_ELASTICSEARCH = CONFIGURATION["ELASTICSEARH_CLUSTER"]["IPS"].split(',')
PORTS_CLUSTER_ELASTICSEARCH = CONFIGURATION["ELASTICSEARH_CLUSTER"]["PORTS"].split(',')


###########################################################
#######  FUNCTIONS
##########################################################

def __set_data(df, index_name, columns, id_doc=None):
    for index, row in df.iterrows():
        data = {"@timestamp": datetime.today().astimezone(tz=pytz.timezone('UTC'))}
        id_value = None
        for column in columns:
            if not pd.isnull(row[column]) and row[column]:
                if type(row[column]) is time:
                    data[column] = "{0:%H:%M:%S}".format(row[column])
                elif type(row[column]) is datetime:
                    data[column] = row[column].astimezone(tz=pytz.timezone('UTC'))
                elif type(row[column]) is pd.Timestamp:
                    data[column] = utils.__to_date("{0:%Y-%m-%d %H:%M:%S}".format(row[column])).astimezone(
                        tz=pytz.timezone('UTC'))
                else:
                    data[column] = row[column]
                if id_doc and column == id_doc:
                    id_value = str(row[column])
        if id_value:
            yield {
                "_index": index_name,
                "_id": id_value,
                "_source": data
            }
        else:
            yield {
                "_index": index_name,
                "_source": data
            }


def __save_data_load(index, df, columns, id_doc=None):
    logger.info("###### __save_data_load ######")
    logger.info("index_name : {0}".format(index))
    es = Elasticsearch(hosts=IPS_CLUSTER_ELASTICSEARCH, ports=PORTS_CLUSTER_ELASTICSEARCH, timeout=600)
    success, _ = bulk(es, __set_data(df, index, columns, id_doc))
    logger.info("success : {0}".format(success))
    return success


def __convert_local_time(date_time):
    if date_time:
        from_zone = pytz.timezone("UTC")
        to_zone = pytz.timezone('America/Mexico_City')
        date_time = from_zone.localize(date_time)
        return date_time.astimezone(to_zone)


def __set_update_data(df, index_name, columns, id_doc='_id', index_column='_index'):
    for index, row in df.iterrows():
        data = {}
        for column in columns:
            if column not in ["_id", "_index"]:
                if not pd.isnull(row[column]) and row[column]:
                    if type(row[column]) is datetime:
                        data[column] = row[column].astimezone(tz=pytz.timezone('UTC'))
                    elif type(row[column]) is pd.Timestamp:
                        data[column] = utils.__to_date("{0:%Y-%m-%d %H:%M:%S}".format(row[column])).astimezone(
                            tz=pytz.timezone('UTC'))
                    else:
                        data[column] = row[column]
        if index_name:
            yield {
                "_index": index_name,
                "_id": row[id_doc],
                "_op_type": "update",
                "doc": data
            }
        elif index_column and index_column in columns:
            yield {
                "_index": row[index_column],
                "_id": row[id_doc],
                "_op_type": "update",
                "doc": data
            }


def __update_data_load(index, df, columns, id_doc='_id', index_column='_index'):
    logger.info("###### __update_data_load ######")
    logger.info("index_name : {0}".format(index))
    es = Elasticsearch(hosts=IPS_CLUSTER_ELASTICSEARCH, ports=PORTS_CLUSTER_ELASTICSEARCH, timeout=600)
    success, _ = bulk(es, __set_update_data(df, index, columns, id_doc, index_column))
    logger.info("success : {0}".format(success))
    return success


def __get_operation_avg(columns):
    agg_avg = {}
    for column in columns:
        agg_avg[column] = ['sum', 'min', 'max', 'count']
    return agg_avg


def __get_operation_desv(columns):
    agg_avg = {}
    for column in columns:
        agg_avg[column] = ['sum', 'count']
    return agg_avg


def __remove_from_list(list_data, list_remove):
    for remove in list_remove:
        list_data.remove(remove)
    return list_data


def __get_dataframe_avg(df_data, keys, columns):
    logger.info("###### __get_dataframe_avg ######")
    logger.info("df_data :{0}".format(len(df_data)))
    logger.info("columns :{0}".format(columns))
    logger.info("keys :{0}".format(keys))
    agg_avg = __get_operation_avg(columns)
    df_data = df_data.groupby(keys).agg(agg_avg).reset_index()
    for column in columns:
        keys.append('{0}_promedio'.format(column))
        keys.append('{0}_min'.format(column))
        keys.append('{0}_max'.format(column))
        df_data['{0}_min'.format(column)] = df_data[column]['min']
        df_data['{0}_max'.format(column)] = df_data[column]['max']
        df_data['{0}_promedio'.format(column)] = df_data[column].apply(
            lambda x: (x['sum'] - (x['min'] + x['max'])) / (x['count'] - 2) if x['count'] > 3 else x['sum'] / x[
                'count'], axis=1)

    df_data.columns = df_data.columns.droplevel(1)
    return df_data[keys]


def __get_dataframe_desv(df_data, df_avg, keys, columns):
    logger.info("###### __get_dataframe_desv ######")
    logger.info("df_data :{0}".format(len(df_data)))
    logger.info("df_avg :{0}".format(len(df_avg)))
    logger.info("columns :{0}".format(columns))
    logger.info("keys :{0}".format(keys))
    df_std = df_data.merge(df_avg, on=keys, how='inner').copy()
    keys_desv = []
    for column in columns:
        keys_desv.append('{0}_desv'.format(column))
        df_std['{0}_desv'.format(column)] = df_std.apply(
            lambda x: 0 if x[column] == x['{0}_min'.format(column)] or x[column] == x['{0}_max'.format(column)] else (x[
                                                                                                                          column] -
                                                                                                                      x[
                                                                                                                          '{0}_promedio'.format(
                                                                                                                              column)]) ** 2,
            axis=1)
    agg_desv = __get_operation_desv(keys_desv)
    df_std = df_std[keys + keys_desv].groupby(keys).agg(agg_desv).reset_index()
    print(keys_desv)
    for column in columns:
        keys.append('{0}_desviacion'.format(column))
        df_std['{0}_desviacion'.format(column)] = df_std['{0}_desv'.format(column)].apply(
            lambda x: math.sqrt(x['sum'] / (x['count'] - 2)) if x['count'] > 3 else math.sqrt(x['sum'] / x['count']),
            axis=1)
    df_std.columns = df_std.columns.droplevel(1)
    return df_std[keys]


def __columns_calculation(columns):
    columns_cal = []
    for column in columns:
        columns_cal.append('{0}_desviacion'.format(column))
        columns_cal.append('{0}_minimo'.format(column))
        columns_cal.append('{0}_maximo'.format(column))
        columns_cal.append('{0}_promedio'.format(column))
    return columns_cal


def __get_field_aggreations(fields, operation):
    logger.info("###### __get_field_aggreations ######")
    field_aggregation = []
    for field in fields:
        field_aggregation.append({'field': field, 'operation': operation})

    return field_aggregation


def __add_field_agregation(fields_agregation):
    logger.info("###### __process_files ######")
    aggs = {}
    if fields_agregation:
        for field in fields_agregation:
            aggs[field['field']] = {field['operation']: {"field": field['field']}}
    return aggs


def __keys_aggreation(fields_agregation):
    keys = []
    if fields_agregation:
        for field in fields_agregation:
            keys.append(field['field'])
    return keys


def __get_field_query(field, position, types):
    if types:
        if len(types) > 1:
            if types[position] != 'keyword':
                return field

    return '{0}.keyword'.format(field)


def __build_query(start_date, end_date, field_time, field_terms, field_types, fields_agregation, interval,
                  filter_group):
    logger.info("###### __get_date_aggregation ######")
    logger.info("start_date :{0}".format(start_date))
    logger.info("end_date :{0}".format(end_date))
    logger.info("field_time :{0}".format(field_time))
    logger.info("field_terms :{0}".format(field_terms))
    logger.info("field_types :{0}".format(field_types))
    logger.info("interval :{0}".format(interval))
    logger.info("fields_agregation :{0}".format(fields_agregation))
    filter_bool = {}
    if filter_group != None:
        filter_bool["must"] = [
            {"term":
                {
                    filter_group.split(':')[0]: filter_group.split(':')[1]
                }
            }
        ]
    filter_bool["filter"] = {
        "range": {
            field_time: {
                "gte": utils.__get_epoch_time_ms(start_date),
                "lte": utils.__get_epoch_time_ms(end_date),
                "format": "epoch_millis"
            }
        }
    }
    es_query = {}
    if len(field_terms) > 1:
        es_query = {
            "query": {
                "bool": filter_bool
            },
            "aggs": {
                field_terms[0]: {
                    "terms": {
                        "field": __get_field_query(field_terms[0], 0, field_types),
                        "size": 10000
                    },
                    "aggs": {
                        field_terms[1]: {
                            "terms": {
                                "field": __get_field_query(field_terms[1], 1, field_types),
                                "size": 10000
                            },
                            "aggs": {
                                field_time: {
                                    "date_histogram": {
                                        "field": field_time,
                                        "fixed_interval": interval
                                    },
                                    "aggs": __add_field_agregation(fields_agregation)
                                }
                            }
                        }
                    }
                }
            }
        }
    else:
        es_query = {
            "query": {
                "bool": filter_bool
            },
            "aggs": {
                field_terms[0]: {
                    "terms": {
                        "field": __get_field_query(field_terms[0], 0, field_types),
                        "size": 10000
                    },
                    "aggs": {
                        field_time: {
                            "date_histogram": {
                                "field": field_time,
                                "fixed_interval": interval
                            },
                            "aggs": __add_field_agregation(fields_agregation)
                        }
                    }
                }
            }
        }

    return es_query


def __get_date_aggregation(index, start_date, end_date, field_time, field_terms, field_types, fields_agregation,
                           interval,
                           filter_group):
    logger.info("###### __get_date_aggregation ######")
    logger.info("start_date :{0}".format(start_date))
    logger.info("end_date :{0}".format(end_date))
    logger.info("index :{0}".format(index))
    logger.info("field_time :{0}".format(field_time))
    logger.info("field_terms :{0}".format(field_terms))
    logger.info("field_types :{0}".format(field_types))
    logger.info("interval :{0}".format(interval))
    logger.info("fields_agregation :{0}".format(fields_agregation))
    logger.info("IPS_CLUSTER_ELASTICSEARCH :{0}".format(IPS_CLUSTER_ELASTICSEARCH))
    logger.info("PORTS_CLUSTER_ELASTICSEARCH :{0}".format(PORTS_CLUSTER_ELASTICSEARCH))
    es = Elasticsearch(hosts=IPS_CLUSTER_ELASTICSEARCH, ports=PORTS_CLUSTER_ELASTICSEARCH, timeout=600)
    logger.info(__add_field_agregation(fields_agregation))

    es_query = __build_query(start_date, end_date, field_time, field_terms, field_types, fields_agregation, interval,
                             filter_group)
    logger.info(es_query)
    res = es.search(index=index, request_timeout=6000, size=0, body=es_query)
    data = {}
    count = 1
    keys = __keys_aggreation(fields_agregation)

    if len(field_terms) > 1:
        for term0 in res["aggregations"][field_terms[0]]["buckets"]:
            for term1 in term0[field_terms[1]]["buckets"]:
                for date_time in term1[field_time]["buckets"]:
                    data[count] = {field_terms[0]: term0['key'],
                                   field_terms[1]: term1['key'],
                                   field_time: date_time['key_as_string']}
                    for key in keys:
                        if key in date_time:
                            data[count][key] = date_time[key]['value']
                    count = count + 1

    else:
        for term in res["aggregations"][field_terms[0]]["buckets"]:
            for date_time in term[field_time]["buckets"]:
                data[count] = {field_terms[0]: term['key'],
                               field_time: date_time['key_as_string']}
                for key in keys:
                    if key in date_time:
                        data[count][key] = date_time[key]['value']
                count = count + 1

    return pd.DataFrame(data).transpose()


def __get_referencia_by_interval(index, days_rest, days_cal, field_time, field_terms, field_types,
                                 fields_values, interval,
                                 operation='sum',
                                 equals_day=True, extract_day=True, filter_group=None):
    logger.info("###### __get_referencia_by_hour ######")
    logger.info("index :{0}".format(index))
    logger.info("days_rest :{0}".format(days_rest))
    logger.info("days_cal :{0}".format(days_cal))
    logger.info("field_time :{0}".format(field_time))
    logger.info("field_terms :{0}".format(field_terms))
    logger.info("field_types :{0}".format(field_types))
    logger.info("fields_values :{0}".format(fields_values))
    logger.info("interval :{0}".format(interval))
    logger.info("operation :{0}".format(operation))
    logger.info("equals_day :{0}".format(equals_day))
    logger.info("extract_day :{0}".format(extract_day))
    logger.info("filter_group :{0}".format(filter_group))
    if equals_day and equals_day == 'True':
        days_sum = 7
    else:
        days_sum = 1
    field_aggregation = __get_field_aggreations(fields_values, operation)
    df_result = pd.DataFrame()
    for day in range(days_rest, days_rest + days_cal + 1, days_sum):
        date_time = datetime.today()
        if extract_day == 'True':
            date_to = "{0:%Y-%m-%d} 23:59:59".format(date_time - timedelta(days=day))
            date_from = "{0:%Y-%m-%d} 00:00:00".format(date_time - timedelta(days=day))

            df_data = __get_date_aggregation(index, date_from, date_to, field_time,
                                             field_terms, field_types, field_aggregation, interval, filter_group)
            df_result = pd.concat([df_data, df_result])
        else:
            for hour in range(0, 24):
                date_to = "{0:%Y-%m-%d} {1:00}:59:59".format(date_time - timedelta(days=day), hour)
                date_from = "{0:%Y-%m-%d} {1:00}:00:00".format(date_time - timedelta(days=day), hour)
                df_data = __get_date_aggregation(index, date_from, date_to, field_time,
                                                 field_terms, field_types, field_aggregation, interval, filter_group)
                df_result = pd.concat([df_data, df_result])
    return df_result


def __get_dataframe_reference_min(df_data, column_time, keys, columns):
    logger.info("###### __get_dataframe_desv ######")
    logger.info("df_data :{0}".format(len(df_data)))
    logger.info("columns :{0}".format(columns))
    logger.info("column_time :{0}".format(column_time))
    logger.info("keys :{0}".format(keys))
    df_data_hour_sum = df_data[[column_time, 'hour'] + columns + keys].groupby(
        [column_time, 'hour'] + keys).sum().reset_index()
    df_data_hour_count = df_data[[column_time, 'hour', columns[-1]] + keys].groupby(
        [column_time, 'hour'] + keys).count().reset_index()
    df_data_hour_count = df_data_hour_count.rename({columns[-1]: 'count'}, axis=1)
    df_data_hour = df_data_hour_sum.merge(df_data_hour_count, on=[column_time, 'hour'] + keys, how='inner')
    df_avg = __get_dataframe_avg(df_data_hour, ['hour'] + keys, columns + ['count'])
    df_desv = __get_dataframe_desv(df_data_hour, df_avg, ['hour'] + keys, columns)
    df_avg_min = __get_dataframe_avg(df_data[['hour', 'minute'] + keys + columns], ['hour', 'minute'] + keys, columns)
    df_desv = df_desv.merge(df_avg_min.copy(), on=['hour'] + keys, how='inner')
    df_desv = df_desv.merge(df_avg[['hour', 'count_promedio'] + keys], on=['hour'] + keys, how='inner')
    for column in columns:
        df_desv['{0}_desviacion'.format(column)] = df_desv['{0}_desviacion'.format(column)] / df_desv['count_promedio']
        df_desv['{0}_minimo'.format(column)] = df_desv['{0}_promedio'.format(column)] - (
                df_desv['{0}_desviacion'.format(column)] * 2)
        df_desv['{0}_maximo'.format(column)] = df_desv['{0}_promedio'.format(column)] + (
                df_desv['{0}_desviacion'.format(column)] * 2)
        df_desv.loc[df_desv['{0}_minimo'.format(column)] < 0, '{0}_minimo'.format(column)] = 0
    return df_desv[['hour', 'minute'] + keys + __columns_calculation(columns)]


def __add_field_calculate(fields_calculate, df_data):
    logger.info("###### __add_field_calculate ######")
    logger.info("fields_calculate :{0}".format(fields_calculate))
    columns = []
    if not df_data.empty:
        if fields_calculate and len(fields_calculate) > 0:
            for field in fields_calculate:
                if field and field != '':
                    field_name = field.split('=')[0]
                    field_operation = field.split('=')[-1]
                    columns.append(field_name)
                    for field_calculate in field_operation.split('+'):
                        if field_name in df_data.columns:
                            df_data[field_name] = df_data[field_name] + df_data[field_calculate]
                        else:
                            df_data[field_name] = df_data[field_calculate]

    return df_data, columns


def __calculate_reference_by_group(days, save_day, index_source, index_final, index_partition, days_cal, field_time,
                                   field_terms, field_types, fields_values, fields_calculate, interval, operation,
                                   extract_day,
                                   equals_day, filters_group):
    logger.info("index_source :{0}".format(index_source))
    logger.info("index_final :{0}".format(index_final))
    logger.info("index_partition :{0}".format(index_partition))
    logger.info("days_cal :{0}".format(days_cal))
    logger.info("field_time :{0}".format(field_time))
    logger.info("field_terms :{0}".format(field_terms))
    logger.info("field_types :{0}".format(field_types))
    logger.info("fields_values :{0}".format(fields_values))
    logger.info("fields_calculate :{0}".format(fields_calculate))
    logger.info("interval :{0}".format(interval))
    logger.info("operation :{0}".format(operation))
    logger.info("extract_day :{0}".format(extract_day))
    logger.info("equals_day :{0}".format(equals_day))
    logger.info("filters_group :{0}".format(filters_group))

    df_index = __get_referencia_by_interval(index_source, days, days_cal, field_time, field_terms, field_types,
                                            fields_values, interval, operation, equals_day, extract_day, filters_group)

    if not df_index.empty:
        df_index[field_time] = pd.to_datetime(df_index[field_time])
        df_index[field_time] = df_index[field_time].apply(__convert_local_time)
        # df_index[field_time] = df_index[field_time].dt.tz_convert('America/Mexico_City')
        for field_term in field_terms:
            df_index[field_term] = df_index[field_term].astype(str)
        df_index['hour'] = df_index[field_time].apply(lambda x: '{0:%H}'.format(x))
        df_index['minute'] = df_index[field_time].apply(lambda x: '{0:%M}'.format(x))
        df_index[field_time] = df_index[field_time].apply(lambda x: '{0:%Y-%m-%d}'.format(x))
        df_index, columns_add = __add_field_calculate(fields_calculate, df_index)
        df_reference = __get_dataframe_reference_min(df_index, field_time, field_terms, fields_values + columns_add)
        df_reference['hour'] = df_reference['hour'].astype(int)
        df_reference['minute'] = df_reference['minute'].astype(int)
        df_reference['hour'] = df_reference['hour'].apply(lambda x: '{0:02d}'.format(x))
        df_reference['minute'] = df_reference['minute'].apply(lambda x: '{0:02d}'.format(x))
        date_save = "{0:%Y-%m-%d}".format(datetime.today() - timedelta(days=days) + timedelta(days=save_day))
        df_reference[field_time] = date_save + ' ' + df_reference['hour'] + ':' + df_reference['minute'] + ':00'
        df_reference[field_time] = pd.to_datetime(df_reference[field_time])
        if filters_group != None:
            df_reference[filters_group.split(':')[0]] = filters_group.split(':')[1]

        if index_partition:
            index_final = index_final + ('_{0:' + index_partition + '}').format(datetime.today()).format(
                datetime.today() - timedelta(days=days) + timedelta(days=save_day))
        else:
            index_final = index_final + '_{0:%Y%m}'.format(datetime.today()).format(
                datetime.today() - timedelta(days=days) + timedelta(days=save_day))

        df_reference = df_reference.drop(['hour', 'minute'], axis=1)
        __save_data_load(index_final.format(datetime.today() - timedelta(days=days) + timedelta(days=save_day)),
                         df_reference, list(df_reference.columns))
    else:
        logger.warning("No existe informacion")


def __calculate_reference(key, days, save_day):
    logger.info("###### __calculate_reference ######")
    logger.info("days :{0}".format(days))
    logger.info("save_day :{0}".format(save_day))
    index_source = CONFIGURATION[key]["INDEX_SOURCE"]
    index_final = CONFIGURATION[key]["INDEX_FINAL"]
    index_partition = CONFIGURATION[key]["INDEX_PARTITION"]
    days_cal = int(CONFIGURATION[key]["DAYS_CALCULATE"])
    field_time = CONFIGURATION[key]["FIELD_TIME"]
    field_terms = CONFIGURATION[key]["FIELD_TERMS"].split(',')

    fields_values = CONFIGURATION[key]["FIELDS_VALUES"].split(',')
    fields_calculate = CONFIGURATION[key]["FIELDS_CACULATE"].split(',')
    operation = CONFIGURATION[key]["OPERATION_EXTRACT"]
    interval = CONFIGURATION[key]["INTERVAL"]
    equals_day = CONFIGURATION[key]["SAME_DAY"]
    extract_day = CONFIGURATION[key]["EXTRACT_DAY"]
    filters_group = None
    field_types = None
    if "FILTERS_GROUP" in CONFIGURATION[key]:
        filters_group = CONFIGURATION[key]["FILTERS_GROUP"]
    if "FIELD_TYPES" in CONFIGURATION[key]:
        field_types = CONFIGURATION[key]["FIELD_TYPES"].split(',')

    if filters_group != None:
        for filter_group in filters_group.split(','):
            __calculate_reference_by_group(days, save_day, index_source, index_final, index_partition, days_cal,
                                           field_time, field_terms, field_types, fields_values, fields_calculate,
                                           interval, operation,
                                           extract_day, equals_day, filter_group)
    else:
        __calculate_reference_by_group(days, save_day, index_source, index_final, index_partition, days_cal, field_time,
                                       field_terms, field_types, fields_values, fields_calculate, interval, operation,
                                       extract_day,
                                       equals_day, None)


@click.group(name='monitoring')
def cli_api():
    pass


@cli_api.command('calculate_reference',
                 help="Inserta la información de la llave asociada a la configuracion para el calculo")
@click.option('--key', prompt="key", help='Key de la configuración')
@click.option('--day', prompt="day", help='Dia del calculo')
@click.option('--save_day', prompt="save_day", help='Dia del guardado')
def calculate_reference(key, day, save_day):
    logger.info("###### calculate_reference ######")
    __calculate_reference(key, int(day), int(save_day))


@cli_api.command('calculate_reference_recarga',
                 help="Inserta la información de la llave asociada a la configuracion para el calculo")
@click.option('--key', prompt="key", help='Key de la configuración')
@click.option('--day_from', prompt="day_from", help='Dia del calculo')
@click.option('--day_to', prompt="day_to", help='Dia del calculo')
@click.option('--save_day', prompt="save_day", help='Dia del guardado')
def calculate_reference(key, day_from, day_to, save_day):
    logger.info("###### calculate_reference ######")
    for day in range(int(day_to), int(day_from) + 1):
        __calculate_reference(key, day, int(save_day))


if __name__ == "__main__":
    cli_api()

