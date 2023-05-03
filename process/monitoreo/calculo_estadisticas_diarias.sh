#!/bin/sh

DATE_TO=$(date)
echo "Log Info: Proceso ssd"
echo "Fecha Inicio: $DATE_TO"
echo "Log Info: Ejecucion de Proceso Python"

/softw/process/monitoreo/calcule_references.sh REFERENCE_XAG_ENTIDAD 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_CISCO_PRIME_TRAFICO 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_TRAFICO_APN_4G 1 2
/softw/process/monitoreo/calcule_references.sh REFERENCE_TRAFICO_APN_2G_3G 1 2
/softw/process/monitoreo/calcule_references.sh REFERENCE_EFICIENCIA_TRAFICO_APN_4G 1 2
/softw/process/monitoreo/calcule_references.sh REFERENCE_SERVICES_AGRUPACION 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_SERVICES_ENTIDAD 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_BALANCEADORES_TRAFICO 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_EOS_ELEM 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_OEM_RECARGA_CANTIDAD 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_OEM_RECARGA_MONTO 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_OEM_RECARGA_GENERAL 1 7
/softw/process/monitoreo/calcule_references.sh REFERENCE_CISC0_PRIME_TRAFICO_DASHBOARD 1 2
#MAX REFERENCE APN TRAFICO
/softw/process/monitoreo/apn/apn_trafico_reference_max.sh

echo "Log Info: Fin de Proceso Python"
