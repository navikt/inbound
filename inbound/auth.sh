#!/bin/bash

#OEBS auth
use="n"
if [ $OEBS_DB_NAVN ]; then
  echo "Current OEBS DB:" $OEBS_DB_NAVN
  echo "Do you still want use the current OEBS database? Y/n"
  read use
  echo ""
  use=${use:-y}
fi

oebs_prod=oebsp
oebs_q1=oebsq1

if [[ -z ${OEBS_DB_NAVN+x} || $use = 'n' ]]; then
  echo "Choose a database:"
  select db in $oebs_prod $oebs_q1 other; do
    break;
  done
  if [ $db = 'other' ]; then
    echo Database:
    read db
    echo ""
  fi
  export OEBS_DB_NAVN=$db
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
use="n"
if [ $SRV_USR ]; then
  echo "Current inbound user:" $SRV_USR
  echo "Do you still want to use the current user? Y/n"
  read  use
  echo ""
  use=${use:-y}
fi

if [ $use = 'n' ]; then
  echo "Inbound username:"
  read username
  export SRV_USR=$username
fi

if [[ -z ${SRV_PWD+x} || $use = 'n' ]]; then
  echo "Inbound password:"
  read -s password
  export SRV_PWD=$password
fi

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

if [[ -z ${REGNSKAP_RAW_DB+x} || $use = 'n' ]]; then
  echo "Choose a database:"
  select db in $prod_db dev_"$prod_db" other; do
    break;
  done
  if [ $db = 'other' ]; then
    echo Database:
    read db
    echo ""
  fi
  export REGNSKAP_RAW_DB=$db
fi

recreate_db=n
if [ $REGNSKAP_RAW_DB != $prod_db ]; then
  echo "Do you want to create / recreate the inbound database: $REGNSKAP_RAW_DB? y/N"
  read recreate_db
  echo ""
  recreate_db=${recreate_db:-n}
fi

if [ $recreate_db = 'y' ]; then
  echo "Here is script you can run in snowflake:"
  echo ""
  echo "create or replace database $REGNSKAP_RAW_DB clone $prod_db;"
  echo "grant usage on database $REGNSKAP_RAW_DB to role "$prod_db"_loader;"
  echo "grant usage on database $REGNSKAP_RAW_DB to role "$prod_db"_transformer;"
  echo ""
  echo "---"
  echo ""
fi
