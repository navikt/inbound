#!/bin/bash

#OEBS auth
use="n"
if [ $OEBS_DSN ]; then
  echo "Current OEBS DB:" $OEBS_DSN
  echo "Do you still want use the current OEBS database? Y/n"
  read use
  echo ""
  use=${use:-y}
fi

oebs_prod="10.53.136.157:1551/oebsp"
oebs_q1="10.53.136.176:1609/oebsq1"

if [[ -z ${OEBS_DSN+x} || $use = 'n' ]]; then
  echo "Choose a database:"
  select db in $oebs_prod $oebs_q1 other; do
    break;
  done
  if [ $db = 'other' ]; then
    echo Database:
    read db
    echo ""
  fi
  export OEBS_DSN=$db
fi

use="n"
if [ $OEBS_USR ]; then
  echo "Current OEBS user:" $OEBS_USR
  echo "Do you still want to use the current user? Y/n"
  read  use
  echo ""
  use=${use:-y}
fi

if [ $use = 'n' ]; then
  echo "OEBS username:"
  read username
  export OEBS_USR=$username
fi

if [[ -z ${OEBS_PWD+x} || $use = 'n' ]]; then
  echo "OEBS password:"
  read -s password
  export OEBS_PWD=$password
fi

#Snowflake auth
export SRV_USR=$DBT_USR
export SRV_PWD=""
export SRV_AUTHENTICATOR="externalbroser"

#Snowflake DB
use="n"
if [ $REGNSKAP_RAW_DB ]; then
  echo "Current inbound DB:" $REGNSKAP_RAW_DB
  echo "Do you still want use the current database? Y/n"
  read use
  echo ""
  use=${use:-y}
fi

prod_db=regnskap_raw
preprod_db=regnskap_raw__preprod
dev_db="dev_"$USER"_regnskap_raw"

if [[ -z ${REGNSKAP_RAW_DB+x} || $use = 'n' ]]; then
  echo "Choose a database:"
  select db in $prod_db $preprod_db $dev_db other; do
    break;
  done
  if [ $db = 'other' ]; then
    echo Database:
    read db
    echo ""
  fi
  export REGNSKAP_RAW_DB=$db
fi
