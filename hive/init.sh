#!/bin/bash
set -e

echo 'Polling HiveServer2 until ready...'
until beeline -u 'jdbc:hive2://hive-server:10000/' -e 'SHOW DATABASES;' 2>&1 | grep -q 'default'; do
  echo 'HiveServer2 not ready yet, retrying in 20s...'
  sleep 20
done

echo 'HiveServer2 is up — running schema init...'
beeline -u 'jdbc:hive2://hive-server:10000/' -f /hive-sql/hive_schema.sql
echo 'Schema init complete.'
