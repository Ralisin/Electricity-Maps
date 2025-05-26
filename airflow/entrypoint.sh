#!/bin/bash
set -e

echo "==> Initializing Airflow DB"
airflow db init

echo "==> Creating default user"
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

echo "==> Starting scheduler in background"
airflow scheduler &

echo "==> Starting webserver"
exec airflow webserver
