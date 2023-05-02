# -*- coding: utf-8 -*-
from flask import request
from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SelectField, HiddenField, DateField, SelectMultipleField
from wtforms.validators import InputRequired, ValidationError,Length
from wtforms.ext.sqlalchemy.fields import QuerySelectMultipleField
from web_app.bin.repository.db_mos.empresas import get_clientes_cisco_core,get_kpis_alarms,get_status_alarms,get_severidad_alarms, get_ip_clientes



class filtersData(FlaskForm):

    #ip_lista_clientes =  get_ip_clientes()

    Clientes = QuerySelectMultipleField("Clientes:", query_factory = lambda : get_clientes_cisco_core(),
                                        allow_blank=True, blank_text="All", get_pk=lambda a: a.Cliente,
                                        get_label= lambda a: a.Cliente)

    Kpis = QuerySelectMultipleField("kpis:",query_factory = lambda :
                                        get_kpis_alarms((request.args.getlist('Clientes') if len(request.args.getlist('Clientes')) > 0 else [x[0] for x in get_ip_clientes()])),
                                        allow_blank=True, blank_text="All", get_pk=lambda a: a.kpi,
                                        get_label= lambda a: a.kpi)


    #Status = QuerySelectMultipleField("status:",query_factory = lambda : get_status_alarms((request.args.getlist('Clientes') if len(request.args.getlist('Clientes')) > 0  else [x['IP'] for x in get_ip_clientes()])),
    #                                    allow_blank=True, blank_text="All", get_pk=lambda a: a.estado,
    #                                    get_label= lambda a: a.estado)

    Status = SelectMultipleField('STATUS', choices=[('0', 'X'), ('1', u'\u2713')])

    Severidad = QuerySelectMultipleField("severidad:",query_factory = lambda : get_severidad_alarms((request.args.getlist('Clientes') if len(request.args.getlist('Clientes')) > 0  else [x[0] for x in get_ip_clientes()])),
                                        allow_blank=True, blank_text="All", get_pk=lambda a: a.severidad,
                                        get_label= lambda a: a.severidad)

