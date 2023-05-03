#!/bin/sh

DATE_TO=$(date)
echo "Log Info: Proceso ssd"
echo "Fecha Inicio: $DATE_TO"
echo "Log Info: Ejecucion de Proceso Python"
export LC_ALL=de_DE.utf-8
export LANG=de_DE.utf-8
echo "Key: $1";
echo "day: $2";

PYTHONPATH="/softw/" python3.6 /softw/process/monitoreo/references/calcule_references.py calculate_reference --key $1 --day $2 || exit 0

echo "Log Info: Fin de Proceso Python"