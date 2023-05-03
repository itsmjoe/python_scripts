# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from pandasticsearch import Select
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from elasticsearch.helpers import bulk
import pytz

from process import CONFIGURATION

###########################################################
#######  CONFIGURATION
###########################################################


IP_ELASTIC = CONFIGURATION["ELASTICSEARH_CLUSTER"]["IPS"].split(',')
PORT_ELASTIC = CONFIGURATION["ELASTICSEARH_CLUSTER"]["PORTS"].split(',')

es = Elasticsearch(hosts=IP_ELASTIC, ports=PORT_ELASTIC, timeout=600)


def __get_epochtime(datestr, format="%Y-%m-%d %H:%M:%S"):
    date_time = __to_date(datestr, format)
    return int(date_time.strftime("%s"))


def __get_epochtime_ms(datestr, format_date="%Y-%m-%d %H:%M:%S"):
    # print("### __get_epochtime_ms ###")
    date_time = __to_date(datestr, format_date)
    return int(date_time.strftime("%s")) * 1000


def __to_date(datestr, format_date="%Y-%m-%d %H:%M:%S"):
    # print("### __to_date ###")
    from datetime import datetime
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format_date)

def __get_data_from_4g(index,date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query={
            "size": 0, 
            "query": {
                "bool": {
                "filter": {
                    "range": {
                    "date_time": {
                        "gte": "2021-12-11T06:35:00+00:00",
                        "lt": "2021-12-11T06:40:00+00:00"
                    }
                    }
                }
                }
            },
            "aggs": {
                "promedios": {
                "terms": {
                    "field": "nename.keyword.keyword"
                },
                "aggs": {
                    "sum_by_sgi_uplink_promedio": {
                    "sum": {
                        "field": "sgi uplink user traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_by_sgi_uplink_promedio_peak": {
                    "sum": {
                        "field": "sgi uplink user traffic peak throughput in kb/s (apn)_promedio"
                    }
                    },
                    "sum_by_sgi_downlink_promedio": {
                    "sum": {
                        "field": "sgi downlink user traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_by_sgi_downlink_promedio_peak": {
                    "sum": {
                        "field": "sgi downlink user traffic peak throughput in kb/s (apn)_promedio"
                    }
                    },
                    "sum_promedio": {
                    "bucket_script": {
                        "buckets_path": {
                        "uplink_1": "sum_by_sgi_uplink_promedio",
                        "uplink_2": "sum_by_sgi_uplink_promedio_peak",
                        "downlink_1": "sum_by_sgi_downlink_promedio",
                        "downlink_2": "sum_by_sgi_downlink_promedio_peak"
                        },
                        "script": "params.uplink_1 + params.uplink_2 + params.downlink_1 + params.downlink_2"
                    }
                    }
                }
                }
            }
        }
    print(query)
    res = es.search(index=index,
                    scroll='100m',
                    size=10000,
                    request_timeout=600,
                    body=query)

    count = 1
    data = {}
# "buckets" : [
#         {
#           "key" : "DFNGG2",
#           "doc_count" : 206,
#           "sum_by_sgi_downlink_promedio" : {
#             "value" : 1415168.0
#           },
#           "sum_by_sgi_uplink_promedio_peak" : {
#             "value" : 4096.0
#           },
#           "sum_by_sgi_uplink_promedio" : {
#             "value" : 1091584.0
#           },
#           "sum_by_sgi_downlink_promedio_peak" : {
#             "value" : 5120.0
#           },
#           "sum_promedio" : {
#             "value" : 2515968.0
#           }
#         }

    for nename in res["aggregations"]["promedios"]["buckets"]:
        data[count] = {"nename": nename["key"], "promedio_4g": nename["sum_promedio"]["value"]}
        count = count + 1
    df = pd.DataFrame(data).transpose()
    print("Got {0} Hits:".format(df.count()))
    return df

def __get_data_from_2g_3g(index,date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query={
            "size": 0, 
            "query": {
                "bool": {
                "filter": {
                    "range": {
                    "date_time": {
                        "gte": "2021-12-11T06:35:00+00:00",
                        "lt": "2021-12-11T06:40:00+00:00"
                    }
                    }
                }
                }
            },
            "aggs": {
                "promedios": {
                "terms": {
                    "field": "nename.keyword.keyword"
                },
                "aggs": {
                    "sum_by_gi_uplink_promedio": {
                    "sum": {
                        "field": "gi uplink traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_by_gn_uplink_promedio": {
                    "sum": {
                        "field": "gn uplink traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_by_gi_downlink_promedio": {
                    "sum": {
                        "field": "gi downlink traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_by_gn_downlink_promedio": {
                    "sum": {
                        "field": "gn downlink traffic in kb (apn)_promedio"
                    }
                    },
                    "sum_promedio": {
                    "bucket_script": {
                        "buckets_path": {
                        "uplink_1": "sum_by_gi_uplink_promedio",
                        "uplink_2": "sum_by_gn_uplink_promedio",
                        "downlink_1": "sum_by_gi_downlink_promedio",
                        "downlink_2": "sum_by_gn_downlink_promedio"
                        },
                        "script": "params.uplink_1 + params.uplink_2 + params.downlink_1 + params.downlink_2"
                    }
                    }
                }
                }
            }
        }
    print(query)
    res = es.search(index=index,
                    scroll='100m',
                    size=10000,
                    request_timeout=600,
                    body=query)

    count = 1
    data = {}

    for nename in res["aggregations"]["promedios"]["buckets"]:
        data[count] = {"nename": nename["key"], "promedio_2g_3g": nename["sum_promedio"]["value"]}
        count = count + 1
    df = pd.DataFrame(data).transpose()
    print("Got {0} Hits:".format(df.count()))
    return df



def __get_data_from_es(index,date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query={
            "_source": ['_id', 'Servidor','bitsOut','bitsIn'],
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "Timestamp": {
                                "gte": date_from,
                                "lte": date_to,
                                "format": "epoch_millis"
                            }
                        }
                    }
                }
            }
        }
    print(query)
    res = es.search(index=index,
                    scroll='100m',
                    size=10000,
                    request_timeout=600,
                    body=query)

    total = res['hits']['total']['value']
    df = Select.from_dict(res).to_pandas()

    if total and total > 0 and not df.empty:
        sid = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
        df = pd.DataFrame()
        print("Got %d Hits:" % res['hits']['total']['value'])
        while scroll_size > 0:
            print("scroll size: " + str(scroll_size))
            pandas_df = Select.from_dict(res).to_pandas()
            df = pd.concat([df, pandas_df], sort=False)
            res = es.scroll(scroll_id=sid, scroll='10m', request_timeout=600)
            sid = res['_scroll_id']
            scroll_size = len(res['hits']['hits'])
        return df

def __restar(x,y):
    if y==None:
        return None
    elif x<y:
        return 0
    else:
        return x-y

# def _processdata(index_name,date_from, date_to,date_from_last, date_to_last):
#     df_balanceadores_ultimo = __get_data_from_es(index_name,date_from, date_to)
#     print(df_balanceadores_ultimo)
#     df_balanceadores_penultimo = __get_data_from_es(index_name,date_from_last, date_to_last)
#     print(df_balanceadores_penultimo)
#     df_balanceadores_penultimo = df_balanceadores_penultimo[['Servidor','bitsOut','bitsIn']]
#     df_balanceadores_penultimo=df_balanceadores_penultimo.rename(columns={"bitsOut":"bitsOut_anterior","bitsIn":'bitsIn_anterior'})
#     df_return=df_balanceadores_ultimo.merge(df_balanceadores_penultimo,on="Servidor",how="left")
#     df_return["traficoIn"]=df_return.apply(lambda x: __restar(x["bitsIn"], x["bitsIn_anterior"]), axis=1)
#     df_return["traficoOut"]=df_return.apply(lambda x: __restar(x["bitsOut"], x["bitsOut_anterior"]), axis=1)

#     return df_return[["_id",'traficoIn','traficoOut']]


def _processdata(index_name_4g,index_name_2g_3g,date_from, date_to):
    df_reference_4g= __get_data_from_4g(index_name_4g,date_from,date_to)
    df_reference_2g_3g= __get_data_from_2g_3g(index_name_2g_3g,date_to,date_from)
    df_return=df_reference_4g.merge(df_reference_2g_3g,on="nename", how="left")
    df_return["promedio"]=df_return['promedio_4g'] + df_return['promedio_2g_3g']
    #df_return["promedio"]=df_return[['promedio_4g', 'promedio_2g_3g']].values.max(1)
    print(df_return)
    # df_balanceadores_ultimo = __get_data_from_es(index_name,date_from, date_to)
    # print(df_balanceadores_ultimo)
    # df_balanceadores_penultimo = __get_data_from_es(index_name,date_from_last, date_to_last)
    # print(df_balanceadores_penultimo)
    # df_balanceadores_penultimo = df_balanceadores_penultimo[['Servidor','bitsOut','bitsIn']]
    # df_balanceadores_penultimo=df_balanceadores_penultimo.rename(columns={"bitsOut":"bitsOut_anterior","bitsIn":'bitsIn_anterior'})
    # df_return=df_balanceadores_ultimo.merge(df_balanceadores_penultimo,on="Servidor",how="left")
    # df_return["traficoIn"]=df_return.apply(lambda x: __restar(x["bitsIn"], x["bitsIn_anterior"]), axis=1)
    # df_return["traficoOut"]=df_return.apply(lambda x: __restar(x["bitsOut"], x["bitsOut_anterior"]), axis=1)

    # return df_return[["_id",'traficoIn','traficoOut']]


def __set_update_data(df, index_name, columns):
    for index, row in df.iterrows():
        data = {}
        for column in columns:
            if column not in ["_id", "_index"]:
                if not pd.isnull(row[column]) and row[column]:
                    if type(row[column]) is datetime:
                        data[column] = row[column].astimezone(tz=pytz.timezone('UTC'))
                    elif type(row[column]) is pd.Timestamp:
                        data[column] = __to_date("{0:%Y-%m-%d %H:%M:%S}".format(row[column])).astimezone(
                            tz=pytz.timezone('UTC'))
                    else:
                        data[column] = row[column]
        yield {
            "_index": index_name,
            "_id": row["_id"],
            "_op_type": "update",
            "doc": data
        }


def __update_data_load(es, df, index_name, columns):
    print("### __update_data_loda ###")
    print("index_name : {0}".format(index_name))
    success, _ = bulk(es, __set_update_data(df, index_name, columns))
    print("success : {0}".format(success))
    return success



def main():
    today = datetime.now()
    print("########## Inicio Ejecucion ##########")
    print("{0}".format(today))
    date_from_format = ("{0:%Y-%m-%d %H:%M}:00".format(today - relativedelta(minutes=5)))
    date_to_format = ("{0:%Y-%m-%d %H:%M}:59".format(today - relativedelta(minutes=1)))
    date_from = __get_epochtime_ms(date_from_format)
    date_to = __get_epochtime_ms(date_to_format)

    # index_name="balanceadores_trafico_5min*"
    index_name_4g="apn_4g_referencia_trafico_5min*"
    index_name_2g_3g="apn_2g_3g_referencia_trafico_5min*"
    # index_name_save="balanceadores_trafico_5min_{0:%Y_%m}".format(today - relativedelta(minutes=1))

    # df_update = _processdata(index_name,date_from, date_to,date_from_last,date_to_last)
    # if len(df_update):
    #     __update_data_load(es, df_update,index_name_save, ["_id",'traficoIn','traficoOut'])

    _processdata(index_name_4g,index_name_2g_3g,date_from,date_to)

    today = datetime.now()
    print("########## Finaliza Ejecucion ##########")
    print("{0}".format(today))

if __name__ == "__main__":
    main()


