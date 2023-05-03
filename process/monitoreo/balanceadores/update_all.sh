#!/bin/sh


fecha=`date  +"%Y-%m-%d-%H"`
echo "Inicio"
echo "$fecha"

PYTHONPATH="/softw/" python3.6 /softw/process/monitoreo/balanceadores/update_all.py

echo "finaliza Python"
