#!/bin/sh


fecha=`date  +"%Y-%m-%d-%H"`
echo "Inicio"
echo "$fecha"

PYTHONPATH="/softw/" python3.6 /softw/process/monitoreo/balanceadores/balanceadores_trafico.py

echo "finaliza Python"
