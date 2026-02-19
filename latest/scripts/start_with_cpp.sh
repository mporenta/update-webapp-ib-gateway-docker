#!/usr/bin/env bash
set -euo pipefail

IB_RUNNER_PID=""
CPP_PID=""
SHUTTING_DOWN=0

cleanup() {
  if [[ "$SHUTTING_DOWN" -eq 1 ]]; then
    return
  fi
  SHUTTING_DOWN=1

  echo "[supervisor] shutting down child processes"

  if [[ -n "$CPP_PID" ]] && kill -0 "$CPP_PID" 2>/dev/null; then
    kill -TERM "$CPP_PID" 2>/dev/null || true
  fi

  if [[ -n "$IB_RUNNER_PID" ]] && kill -0 "$IB_RUNNER_PID" 2>/dev/null; then
    kill -TERM "$IB_RUNNER_PID" 2>/dev/null || true
  fi

  wait "$CPP_PID" 2>/dev/null || true
  wait "$IB_RUNNER_PID" 2>/dev/null || true
}

trap 'cleanup; exit 0' SIGINT SIGTERM

CPP_IB_GATEWAY_HOST="${CPP_IB_GATEWAY_HOST:-localhost}"
CPP_IB_GATEWAY_PORT="${CPP_IB_GATEWAY_PORT:-4002}"
CPP_HTTP_PORT="${CPP_HTTP_PORT:-8088}"
CPP_IB_CLIENT_ID="${CPP_IB_CLIENT_ID:-99}"
CPP_REDIS_HOST="${CPP_REDIS_HOST:-redis}"
CPP_REDIS_PORT="${CPP_REDIS_PORT:-6379}"
CPP_WAIT_FOR_IB_SECONDS="${CPP_WAIT_FOR_IB_SECONDS:-300}"

if [[ "$CPP_IB_GATEWAY_HOST" != "localhost" && "$CPP_IB_GATEWAY_HOST" != "127.0.0.1" ]]; then
  echo "[supervisor] warning: CPP_IB_GATEWAY_HOST is '$CPP_IB_GATEWAY_HOST'; expected localhost for in-container IB connection"
fi

echo "[supervisor] starting IB Gateway bootstrap"
/root/scripts/run.sh &
IB_RUNNER_PID=$!

echo "[supervisor] waiting for full IB readiness via /root/scripts/ib_health_check.sh"
start_ts=$(date +%s)
while true; do
  if ! kill -0 "$IB_RUNNER_PID" 2>/dev/null; then
    echo "[supervisor] ib-gateway startup process exited before readiness"
    wait "$IB_RUNNER_PID" || true
    exit 1
  fi

  if /root/scripts/ib_health_check.sh >/tmp/ib_startup_health.log 2>&1; then
    echo "[supervisor] IB is fully ready"
    break
  fi

  now_ts=$(date +%s)
  if (( now_ts - start_ts >= CPP_WAIT_FOR_IB_SECONDS )); then
    echo "[supervisor] timed out waiting for IB readiness after ${CPP_WAIT_FOR_IB_SECONDS}s"
    echo "[supervisor] last health check output:"
    tail -n 30 /tmp/ib_startup_health.log || true
    cleanup
    exit 1
  fi

  sleep 5
done

echo "[supervisor] starting embedded cpp-layer: /app/ib_data_server ${CPP_IB_GATEWAY_HOST} ${CPP_IB_GATEWAY_PORT} ${CPP_HTTP_PORT} ${CPP_IB_CLIENT_ID} ${CPP_REDIS_HOST} ${CPP_REDIS_PORT}"
/app/ib_data_server \
  "${CPP_IB_GATEWAY_HOST}" \
  "${CPP_IB_GATEWAY_PORT}" \
  "${CPP_HTTP_PORT}" \
  "${CPP_IB_CLIENT_ID}" \
  "${CPP_REDIS_HOST}" \
  "${CPP_REDIS_PORT}" &
CPP_PID=$!

while true; do
  if ! kill -0 "$IB_RUNNER_PID" 2>/dev/null; then
    echo "[supervisor] ib-gateway process exited unexpectedly"
    cleanup
    exit 1
  fi

  if ! kill -0 "$CPP_PID" 2>/dev/null; then
    echo "[supervisor] embedded cpp-layer process exited unexpectedly"
    cleanup
    exit 1
  fi

  sleep 2
done
