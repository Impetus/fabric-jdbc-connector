#!/bin/bash
sleep 10
while !(mysqladmin -uroot -proot ping)
do
   sleep 5
   echo "waiting for mysql ..."
done
#mysql -uroot  < /db_scripts/test_schema.sql
