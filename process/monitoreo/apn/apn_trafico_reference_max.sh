#!/bin/sh


fecha=`date  +"%Y-%m-%d-%H"`
echo "Inicio"
echo "$fecha"

PYTHONPATH="/softw/" python3.6 /softw/process/monitoreo/apn/apn_trafico_reference_max.py

echo "finaliza Python"
