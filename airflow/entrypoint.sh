#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs \
  /opt/airflow/reports

chmod 777 \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs \
  /opt/airflow/reports

AIRFLOW_USER_NAME="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
AIRFLOW_USER_PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

run_as_airflow() {
  /usr/sbin/runuser --user airflow -- "$@"
}

run_as_airflow airflow db migrate
if ! run_as_airflow airflow users list --output json | python -c 'import json,sys; sys.exit(not any(u["username"] == sys.argv[1] for u in json.load(sys.stdin)))' "${AIRFLOW_USER_NAME}"; then
run_as_airflow airflow users create \
  --username "${AIRFLOW_USER_NAME}" \
  --password "${AIRFLOW_USER_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
fi

/usr/sbin/runuser --user airflow -- airflow scheduler --skip-serve-logs &
scheduler_pid=$!
/usr/sbin/runuser --user airflow -- airflow webserver &
webserver_pid=$!
cleanup() {
  kill -TERM "$scheduler_pid" "$webserver_pid" 2>/dev/null || true
  wait "$scheduler_pid" "$webserver_pid" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 0' TERM INT
wait -n "$scheduler_pid" "$webserver_pid"
