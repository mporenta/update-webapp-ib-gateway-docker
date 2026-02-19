#!/usr/bin/env bash
set -euo pipefail

/root/scripts/ib_health_check.sh
curl -sf "http://localhost:${CPP_HTTP_PORT:-8088}/health" >/dev/null
