# -*- coding: utf-8 -*-

from datetime import datetime
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pandasticsearch import Select
from process import CONFIGURATION

###########################################################
#######  CONFIGURATION
###########################################################


IP_ELASTIC = CONFIGURATION["ELASTICSEARH_CLUSTER"]["IPS"].split(',')
PORT_ELASTIC = CONFIGURATION["ELASTICSEARH_CLUSTER"]["PORTS"].split(',')

es = Elasticsearch(hosts=IP_ELASTIC, ports=PORT_ELASTIC, timeout=600)


def __get_epochtime(datestr, format="%Y-%m-%d %H:%M:%S"):
    date_time = __to_date(datestr, format)
    return int(date_time.timestamp())


def __get_epochtime_ms(datestr, format_date="%Y-%m-%d %H:%M:%S"):
    # print("### __get_epochtime_ms ###")
    date_time = __to_date(datestr, format_date)
    return int(date_time.timestamp()) * 1000


def __to_date(datestr, format_date="%Y-%m-%d %H:%M:%S"):
    # print("### __to_date ###")
    from datetime import datetime
    if not datestr:
        return datetime.today().date()
    return datetime.strptime(datestr, format_date)


def __get_data_from_es(index, date_from, date_to):
    print("### __excute_query_elastic {0} ###".format(index))
    query = {
        "_source": ['_id', 'Servidor', 'bitsOut', 'bitsIn'],
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


def __restar(x, y):
    if y is None:
        return None
    elif x < y:
        return 0
    else:
        return x - y


def _processdata(index_name, date_from, date_to, date_from_last, date_to_last):
    df_balanceadores_ultimo = __get_data_from_es(index_name, date_from, date_to)
    print(df_balanceadores_ultimo)
    df_balanceadores_penultimo = __get_data_from_es(index_name, date_from_last, date_to_last)
    print(df_balanceadores_penultimo)
    df_balanceadores_penultimo = df_balanceadores_penultimo[['Servidor', 'bitsOut', 'bitsIn']]
    df_balanceadores_penultimo = df_balanceadores_penultimo.rename(
        columns={"bitsOut": "bitsOut_anterior", "bitsIn": 'bitsIn_anterior'})
    df_return = df_balanceadores_ultimo.merge(df_balanceadores_penultimo, on="Servidor", how="left")
    df_return["traficoIn"] = df_return.apply(lambda x: __restar(x["bitsIn"], x["bitsIn_anterior"]), axis=1)
    df_return["traficoOut"] = df_return.apply(lambda x: __restar(x["bitsOut"], x["bitsOut_anterior"]), axis=1)
    df_return['traficoTotal'] = df_return['traficoIn'].sum()

    return df_return[["_id", 'traficoIn', 'traficoOut', 'traficoTotal']]


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
    date_from_format_last = ("{0:%Y-%m-%d %H:%M}:00".format(today - relativedelta(minutes=10)))
    date_to_format_last = ("{0:%Y-%m-%d %H:%M}:59".format(today - relativedelta(minutes=6)))
    date_from = __get_epochtime_ms(date_from_format)
    date_to = __get_epochtime_ms(date_to_format)
    date_from_last = __get_epochtime_ms(date_from_format_last)
    date_to_last = __get_epochtime_ms(date_to_format_last)

    index_name = "balanceadores_trafico_5min*"
    index_name_save = "balanceadores_trafico_5min_{0:%Y_%m}".format(today - relativedelta(minutes=1))

    df_update = _processdata(index_name, date_from, date_to, date_from_last, date_to_last)
    if len(df_update):
        __update_data_load(es, df_update, index_name_save, ["_id", 'traficoIn', 'traficoOut', 'traficoTotal'])

    today = datetime.now()
    print("########## Finaliza Ejecucion ##########")
    print("{0}".format(today))


if __name__ == "__main__":
    main()
