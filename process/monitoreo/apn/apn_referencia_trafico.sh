#!/bin/sh


fecha=`date  +"%Y-%m-%d-%H"`
echo "Inicio"
echo "$fecha"

PYTHONPATH="/softw/" python3.6 /softw/process/monitoreo/apn/apn_referencia_trafico.py

echo "finaliza Python"
