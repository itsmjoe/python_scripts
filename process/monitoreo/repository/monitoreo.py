# -*- coding: utf-8 -*-
import pandas as pd
import sqlite3
import mysql.connector
from sqlalchemy import create_engine
import logging
from process.monitoreo.repository import DB_CONFIGURATION_STRING, DB_CONFIGURATION_USER, DB_CONFIGURATION_PASS, \
    DB_CONFIGURATION_DB, DB_CONFIGURATION_IP, DB_CONFIGURATION_PORT


###########################################################
#######  COFIGURATION
###########################################################

logger = logging.getLogger(__name__)

SQL_CAT_INTERFACES_CISCO_CORE = 'Select * From cat_interfaces_cisco_core'

SQL_CAT_INTERFACES_IPRAN = 'Select * From cat_interfaces_ipran'

SQL_CAT_ENTIDADES_SERVICE = "Select * From cat_entidades_service where status = 'ACT' and contenedor = 'CiscoPrime' "

SQL_CAT_EXTRACT_INTERFACE = "Select * from cat_extraccion_interface where pipeline='{0}' and activo"

SQL_CAT_IP = "Select * from cat_ip where Estado='ACT' "

SQL_CAT_INTERFACE_DASH= 'Select * from cisco_prime_cat_interface_dash'

SQL_CAT_ENTIDADES_XAG = "Select * From cat_entidades_xag "

SQL_XAG_ERROR = "select cast(Code as char) as err_code, Code_meaning as code_description from xag_error_code_list "

SQL_CAT_SUCURSAL_ACTIVE= "select * from cisco_core_cat_sucursal"

SQL_CAT_SUCURSAL= "select * from cisco_core_cat_sucursal"

SQL_SNMP_IPS_NODOS = "select * from cat_snmp_ips_nodos where status = 'ACT'"

SQL_SNMP_NET_MEMBERS_DELETE_BY_NODO = "delete from snmp_net_members where Node = '{0}'"

SQL_SNMP_IPS_NODOS_RED_CORP_DEVICES = "select * from cat_snmp_ips_nodos_red_corp where status = 'ACT'"

###########################################################
#######  FUNCTIONS
###########################################################

def __get_cat_sucursal_cisco_core():
    logger.info("###### __get_cat_interfaces_cisco_core ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_SUCURSAL)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()

def __get_cat_interfaces_cisco_core():
    logger.info("###### __get_cat_interfaces_cisco_core ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_INTERFACES_CISCO_CORE)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_ip_cisco_prime():
    logger.info("###### __get_cat_ip_cisco_prime ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_IP)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_interface_dash_cisco_prime():
    logger.info("###### __get_cat_interface_dash_cisco_prime ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_INTERFACE_DASH)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_interfaces_ipran():
    logger.info("###### __get_cat_interfaces_cisco_core ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()    
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_INTERFACES_IPRAN)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_entidades_service():
    logger.info("###### __get_cat_entidades_service ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_ENTIDADES_SERVICE)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_extraccion_interface(pipeline):
    logger.info("###### __get_cat_interfaces_cisco_core ######")
    logger.info("Read file :{0}".format(pipeline))
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_EXTRACT_INTERFACE.format(pipeline))
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()

def __get_cat_entidades_xag():
    logger.info("###### __get_cat_entidades_xag ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_CAT_ENTIDADES_XAG)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()

def __get_cat_snmp_ips():
    logger.info("###### __get_cat_snmp_ips ######")
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_SNMP_IPS_NODOS)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_cat_snmp_ips_red_corp_devices():
    logger.info("###### __get_cat_snmp_ips ######")
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_SNMP_IPS_NODOS_RED_CORP_DEVICES)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __get_xag_error():
    logger.info("###### __get_xag_error ######")
    #conn = sqlite3.connect(DB_CONFIGURATION_STRING)
    conn = __get_connection_configuration()
    try:
        return pd.read_sql(con=conn, sql=SQL_XAG_ERROR)
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        conn.close()


def __save_dataframe_snmp_net_members(dataframe, node):
    logger.info("### __save_dataframe ###")
    __delete_from_snmp_net_members_by_node(node)
    conn = __get_engine_configuration()
    try:
        dataframe.to_sql('snmp_net_members', con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.error("Exception :", e)
        raise e


def __delete_from_snmp_net_members_by_node(node):
    logger.info("### __delete_from_snmp_net_members_by_node ###")
    logger.info("Node: " + node)
    conn = __get_connection_configuration()
    cur = conn.cursor()
    try:
        cur.execute(SQL_SNMP_NET_MEMBERS_DELETE_BY_NODO.format(node))
        conn.commit()
    except Exception as e:
        logger.error("Exception :", e)
        raise e
    finally:
        cur.close()
        conn.close()


def __get_connection_configuration():
    logger.info("###### __get_connection_configuration ######")

    return mysql.connector.connect(host=DB_CONFIGURATION_IP, port=DB_CONFIGURATION_PORT,
                                   user=DB_CONFIGURATION_USER,
                                   database=DB_CONFIGURATION_DB,
                                   password=DB_CONFIGURATION_PASS)


def __get_engine_configuration():
    logger.info("###### __get_connection_configuration ######")
    return create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}'.format(user=DB_CONFIGURATION_USER,
                                                                                 password=DB_CONFIGURATION_PASS,
                                                                                 host=DB_CONFIGURATION_IP,
                                                                                 port=DB_CONFIGURATION_PORT,
                                                                                 schema=DB_CONFIGURATION_DB),
        echo=False)


