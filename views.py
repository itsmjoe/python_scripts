# -*- coding: utf-8 -*-
import json
import logging
from web_app import app
from flask import render_template, Blueprint, request,Response
from flask_paginate import get_page_parameter, Pagination
from .forms import filtersData
from web_app.bin.utils.utils import join_elementos
from web_app.bin.repository.db_mos.empresas import *
import pandas as pd
from datetime import datetime


empresas_blueprint = Blueprint('empresas', __name__)

items_for_page = app.config["ITEMS_FOR_PAGE"]

logger = logging.getLogger(__name__)

#DASHBOARD_LINK = "http://10.225.245.238:3000/d/TBhTmmo7z/estadisticas-por-cliente-sucursal-ip?orgId=5"
DASHBOARD_LINK = "http://10.225.245.238:3000/d/TBhTmmo7z/estadisticas-por-cliente-sucursal-ip?orgId=5&var-cliente={}&var-sucursal={}&var-ip={}"


def concat_path_report(cliente,sucursal,ip):
    sucursal = sucursal.replace(" ","%20")
    return DASHBOARD_LINK.format(cliente,sucursal,ip)

#http://10.225.245.238:3000/d/TBhTmmo7z/estadisticas-por-cliente-sucursal-ip?orgId=5&var-cliente=ABENGOA&var-sucursal=ANZURES%20MIGUEL%20HIDALGO&var-ip=201.166.133.150


@empresas_blueprint.route("/dashboard_alarmas_empresas", methods=["GET", "POST"])
def dashboard_alarmas():
    info = request.args.get('info', None)
    error = request.args.get('error', None)
    form = filtersData(request.args)
    user_login = request.args.get("user_login")
    page = request.args.get(get_page_parameter(), type=int, default=1)

    # parametros de entrada
    status_param = request.args.get("Status")
    # status_param = 1 if status_param == None else int(status_param)
    severidad_param = request.args.getlist("Severidad")
    kpis_param = request.args.getlist("Kpis")
    clientes_param = request.args.getlist("Clientes")

    # status = [state[0] for state in status] if status is not None
    clientes = get_clientes_cisco_core_bylist(clientes_param)

    lista_ips = (cliente.IP for cliente in clientes)

    alarmas, count = get_alarmas_by_cliente(lista_ips, severidad_param, status_param, kpis_param, page, items_for_page)

    camps = ['id', 'fecha_inicio', 'severidad', 'elemento', 'kpi', 'valor_out', 'adicional', 'nota', 'akn']

    campos_clientes = ['IP', 'Cliente', 'DisplayName', 'Sucursal']

    if len(alarmas) == 0:
        df_final = []
        count = 0

    if len(alarmas) != 0:
        df_final = join_elementos({'data': alarmas, 'key': camps}, {'data': clientes, 'key': campos_clientes},
                                  "elemento", "IP", True)

        df_final['akn'] = df_final['akn'].apply(lambda x: ('X' if x == 0 else u'\u2713'))
        df_final["elemento__link"] = df_final.apply(
            lambda row: concat_path_report(row['Cliente'], row['Sucursal'], row['IP']), axis=1)
        # df_final[["Cliente","Sucursal","IP"]].apply(lambda row: concat_path_report(row))#DASHBOARD_LINK.format(df_final["Cliente"],df_final["Sucursal"].replace(" ","%20"), df_final['IP'])

        df_final = df_final.fillna("")

        df_final = df_final.to_records()

    camps = ['akn', 'fecha_inicio', 'severidad', 'elemento', 'kpi', 'valor_out', 'adicional', 'nota', 'Cliente',
             'DisplayName']
    titles = ['STATUS', 'FECHA', 'SEVERIDAD', 'DEVICE', 'KPI', 'VALOR', 'INF. ADICIONAL', 'NOTA OPERADOR', 'CLIENTE',
              'DESCRIPCION', 'COMENTA']
    actions = [
        {'class': 'btn btn-default btn-sm right', 'key': 'id', 'system': 'empresas', 'type': 'button'}
    ]

    urlNew = {}
    pagination = Pagination(css_framework='bootstrap3', page=page, total=count,
                            per_page_parameter=items_for_page, anchor="page",
                            record_name='alarm')
    return render_template('views/db_mos/dashboard_alarmas_empresas/monitoreo.html',
                           form=form, info=info, error=error, data=df_final, pagination=pagination, count=count,
                           camps=camps, titles=titles, actions=actions, urlNew=urlNew, user_login=user_login)


@empresas_blueprint.route("/download_alarmas_empresas", methods=["GET","POST"])
def download_alarmas():
    info = request.args.get('info', None)
    error = request.args.get('error', None)
    form = filtersData(request.args)
    date = datetime.now()

    page = request.args.get(get_page_parameter(), type=int, default=1)

    #parametros de entrada
    status_param = request.args.get("Status")
    status_param = 1 if status_param == None else int(status_param)
    severidad_param = request.args.getlist("Severidad")
    kpis_param = request.args.getlist("Kpis")
    clientes_param = request.args.getlist("Clientes")

    #status = [state[0] for state in status] if status is not None
    clientes = get_clientes_cisco_core_bylist(clientes_param)

    lista_ips = (cliente.IP for cliente in clientes)

    alarmas = get_alarmas_by_cliente_to_download(lista_ips,severidad_param,status_param,kpis_param)

    camps = ['id','fecha_inicio','severidad','elemento','kpi','valor_out','adicional','nota','akn']

    campos_clientes= ['IP','Cliente','DisplayName']

    actions = [
        {'class': 'btn btn-default btn-sm right', 'key': 'id', 'type': 'button'}
    ]

    if len(alarmas) == 0:
        df_final = []
        count = 0
    else:
        df_final = join_elementos({'data': alarmas, 'key': camps},{'data': clientes, 'key': campos_clientes},"elemento","IP",True)
        df_final['akn'] = df_final['akn'].apply(lambda x: ('X' if x == 0 else '✔'))
        #df_final.sort_values(by=["DisplayName"], inplace=True, ascending=False)


    columnsToWrite = ['akn', 'fecha_inicio', 'severidad', 'elemento', 'kpi', 'valor_out', 'adicional', 'nota', 'Cliente',
             'DisplayName']
    columnNames = ['STATUS', 'FECHA', 'SEVERIDAD', 'DEVICE', 'KPI', 'VALOR', 'INF. ADICIONAL', 'NOTA OPERADOR', 'CLIENTE',
              'DESCRIPCION']

    return Response(
        df_final.to_csv(columns=columnsToWrite, header=columnNames, index=False),
        mimetype="text/csv",
        headers={"Content-disposition":
                     "attachment; filename=Alarmas_dns_viprion_" + date.strftime("%Y-%m-%d-%H-%M-%S") + ".csv"})