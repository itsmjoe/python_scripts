# -*- coding: utf-8 -*-

from pandas.core.frame import DataFrame
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


def __get_data_from_es_2g_3g(index,date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query={
            "_source": ["gi downlink traffic in kb (apn)_promedio","gi uplink traffic in kb (apn)_promedio","gn downlink traffic in kb (apn)_promedio","gn uplink traffic in kb (apn)_promedio","nename.keyword","objectmemname0","date_time"], 
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "date_time": {
                                "gte": date_from,
                                "lt": date_to,
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

def __get_data_from_es_4g(index,date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query={
            "_source": ["sgi downlink user traffic in kb (apn)_promedio","sgi downlink user traffic peak throughput in kb/s (apn)_promedio","sgi uplink user traffic in kb (apn)_promedio","sgi uplink user traffic peak throughput in kb/s (apn)_promedio","nename.keyword","objectmemname0","date_time"], 
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "date_time": {
                                "gte": date_from,
                                "lt": date_to,
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

def _processdata(index_name_4g,index_name_2g_3g,date_from, date_to):
    df_reference_4g= __get_data_from_es_4g(index_name_4g,date_from,date_to)
    df_reference_2g_3g= __get_data_from_es_2g_3g(index_name_2g_3g,date_from,date_to)
    df_promedios_4g = None
    df_promedios_2g_3g = None
    df_return = None
    if len(df_reference_4g):
        df_reference_4g = df_reference_4g.dropna(subset=['nename.keyword'])
        df_reference_4g = df_reference_4g.fillna(0)
        df_reference_4g = df_reference_4g[['date_time','nename.keyword','objectmemname0','sgi downlink user traffic in kb (apn)_promedio','sgi downlink user traffic peak throughput in kb/s (apn)_promedio','sgi uplink user traffic in kb (apn)_promedio','sgi uplink user traffic peak throughput in kb/s (apn)_promedio']]
        df_promedios_4g= df_reference_4g.groupby(['nename.keyword','objectmemname0','date_time']).agg({'sgi downlink user traffic in kb (apn)_promedio': "sum",'sgi downlink user traffic peak throughput in kb/s (apn)_promedio': "sum",'sgi uplink user traffic in kb (apn)_promedio': "sum",'sgi uplink user traffic peak throughput in kb/s (apn)_promedio': "sum"})
        df_promedios_4g['promedio_4g']= df_promedios_4g['sgi downlink user traffic in kb (apn)_promedio'] + df_promedios_4g['sgi downlink user traffic peak throughput in kb/s (apn)_promedio'] + df_promedios_4g['sgi uplink user traffic in kb (apn)_promedio'] + df_promedios_4g['sgi uplink user traffic peak throughput in kb/s (apn)_promedio']
    if len(df_reference_2g_3g):
        df_reference_2g_3g = df_reference_2g_3g.dropna(subset=['nename.keyword'])
        df_reference_2g_3g = df_reference_2g_3g.fillna(0)
        df_reference_2g_3g = df_reference_2g_3g[['date_time','nename.keyword','objectmemname0','gi downlink traffic in kb (apn)_promedio','gi uplink traffic in kb (apn)_promedio','gn downlink traffic in kb (apn)_promedio','gn uplink traffic in kb (apn)_promedio']]
        df_promedios_2g_3g= df_reference_2g_3g.groupby(['nename.keyword','objectmemname0','date_time']).agg({'gi downlink traffic in kb (apn)_promedio': "sum",'gi uplink traffic in kb (apn)_promedio': "sum",'gn downlink traffic in kb (apn)_promedio': "sum",'gn uplink traffic in kb (apn)_promedio': "sum"})
        df_promedios_2g_3g['promedio_2g_3g']= df_promedios_2g_3g['gi downlink traffic in kb (apn)_promedio'] + df_promedios_2g_3g['gi uplink traffic in kb (apn)_promedio'] + df_promedios_2g_3g['gn downlink traffic in kb (apn)_promedio'] + df_promedios_2g_3g['gn uplink traffic in kb (apn)_promedio']
    if df_promedios_4g is not None and df_promedios_2g_3g is not None:
        df_return = df_promedios_4g.merge(df_promedios_2g_3g,on=["nename.keyword","objectmemname0","date_time"], how="left")
        df_return = df_return.fillna(0)
        df_return['promedio'] = df_return['promedio_4g'] + df_return['promedio_2g_3g']
    elif(df_promedios_4g is not None):
        df_return = df_promedios_4g
        df_return = df_return.fillna(0)
        df_return['promedio'] = df_return['promedio_4g']
    elif(df_promedios_2g_3g is not None):
        df_return = df_promedios_2g_3g
        df_return = df_return.fillna(0)
        df_return['promedio'] = df_return['promedio_2g_3g']

    if(df_return is not None):
        df_return = df_return.reset_index(level=[0,1])
        df_return = df_return.reset_index(level=[0,0])
        return df_return[['nename.keyword','objectmemname0','date_time','promedio']]
        



def __set_data(df, index_name, columns):
    for index, row in df.iterrows():
        data = {"@timestamp": datetime.today().astimezone(tz=pytz.timezone('UTC'))}
        for column in columns:
            if not pd.isnull(row[column]) and row[column]:
                if type(row[column]) is pd.Timestamp:
                    data[column] = __to_date("{0:%Y-%m-%d %H:%M:%S}".format(row[column]))
                else:
                    data[column] = row[column]
        yield {
            "_index": index_name,
            "_source": data
        }


def __save_data_load(es, df, index_name, columns):
    print("### __save_data_load ###")

    success, _ = bulk(es, __set_data(df, index_name, columns), request_timeout=600)
    return success

def main():
    days_to=6
    today = datetime.now()
    print("########## Inicio Ejecucion ##########")
    print("{0}".format(today))
    date_from_format = ("{0:%Y-%m-%d }00:00:00".format(today + relativedelta(days=days_to)))
    date_to_format = ("{0:%Y-%m-%d }23:59:59".format(today + relativedelta(days=days_to)))
    date_from = __get_epochtime_ms(date_from_format)
    date_to = __get_epochtime_ms(date_to_format)

    index_name_save="apn_referencia_max_trafico_{0:%Y_%m}".format(today - relativedelta(days=days_to))

    index_name_4g="apn_4g_referencia_trafico_5min*"
    index_name_2g_3g="apn_2g_3g_referencia_trafico_5min*"

    df_insert = _processdata(index_name_4g,index_name_2g_3g,date_from,date_to)
    if df_insert is not None:
        __save_data_load(es,df_insert,index_name_save,list(df_insert.columns))
    else:
        print("########## No Data ##########")
    today = datetime.now()
    print("########## Finaliza Ejecucion ##########")
    print("{0}".format(today))

if __name__ == "__main__":
    main()


