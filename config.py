# -*- coding: utf-8 -*-
#################
#### imports ####
#################
from datetime import timedelta

from flask import Flask, render_template, make_response, jsonify
from flask_bcrypt import Bcrypt
from flask_sqlalchemy import SQLAlchemy
from flask_babel import Babel
from web_app.bin.utils.utils import get_app_base_path
from web_app.config import configure_app
from flask import session
from flask_jsglue import JSGlue

################
#### config ####
################

app = Flask(__name__,
            instance_path=get_app_base_path(),
            instance_relative_config=True,
            template_folder='templates')

configure_app(app, 'development')
bcrypt = Bcrypt(app)
babel = Babel(app)
db = SQLAlchemy(app)
jsglue = JSGlue(app)

####################
#### blueprints ####
####################

# MYSQL - DBMOS
from web_app.bin.views.db_mos.cat_extraccion_interface.views import db_mos_cat_interface_blueprint
from web_app.bin.views.db_mos.servicio_cisco_core.views import db_mos_servicio_core_blueprint
from web_app.bin.views.db_mos.servicio_cisco_prime.views import db_mos_servicio_prime_blueprint
from web_app.bin.views.db_mos.dashboard_alarmas_xag.views import db_mos_dashboard_alarmas_xag
from web_app.bin.views.db_mos.dashboard_alarmas_oem.views import db_mos_dashboard_alarmas_oem
from web_app.bin.views.db_mos.dashboard_alarmas_trafico_datos.views import db_mos_dashboard_alarmas_trafico_datos
from web_app.bin.views.db_mos.dashboard_alarms_eos.views import eos_blueprint
from web_app.bin.views.db_mos.dashboard_alarmas_servicios.views import db_mos_dashboard_alarmas_servicios
from web_app.bin.views.db_mos.registro_dispositivos.views import db_mos_registro_dispositivos_blueprint
from web_app.bin.views.db_mos.dashboard_alarmas_empresas.views import empresas_blueprint
from web_app.bin.views.db_mos.dashboard_alarmas_viprion.views import dns_viprion


# MYSQL - DBMOS
app.register_blueprint(db_mos_cat_interface_blueprint)
app.register_blueprint(db_mos_servicio_core_blueprint)
app.register_blueprint(db_mos_servicio_prime_blueprint)
app.register_blueprint(db_mos_dashboard_alarmas_xag)
app.register_blueprint(db_mos_dashboard_alarmas_oem)
app.register_blueprint(db_mos_dashboard_alarmas_trafico_datos)
app.register_blueprint(eos_blueprint)
app.register_blueprint(db_mos_dashboard_alarmas_servicios)
app.register_blueprint(db_mos_registro_dispositivos_blueprint)
app.register_blueprint(empresas_blueprint)
app.register_blueprint(dns_viprion)


############################
#### custom error pages ####
############################

@app.before_request
def before_request():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=30)


@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


@app.errorhandler(400)
def page_not_found(e):
    return make_response(jsonify({'error': 'Not found'}), 400)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('/views/errors/404.html'), 404


@app.errorhandler(403)
def page_not_found(e):
    return render_template('/views/errors/403.html'), 403


@app.errorhandler(410)
def page_not_found(e):
    return render_template('/views/errors/410.html'), 410


@app.template_filter('strftime')
def _jinja2_filter_datetime(date, format_date='%d/%m/%Y %H:%M:%S'):
    if date:
        native = date.replace(tzinfo=None)
        return native.strftime(format_date)
    else:
        return ""


@app.template_filter('history_status')
def _jinja2_filter_history_status(status):
    status_label = {"SUCCESS": "EXITOSO", "ERROR": "ERROR", "PROCESSING": "EN PROCESO"}
    if status:
        if status in status_label:
            return status_label[status]
        else:
            return ""
    else:
        return ""
