#!/usr/bin/env bash
set -euo pipefail

# Periodic container health checks must be cheap and non-invasive.
# The older implementation called /root/scripts/ib_health_check.sh, which runs a
# Python script that fetches historical bars from IB. During long analyzer
# /bars/batch requests this competes for IB resources, fails intermittently, and
# can mark the container unhealthy (triggering autoheal restarts mid-run).
#
# For the embedded cpp-layer image, the /ready endpoint already checks:
#   - IB provider connectivity
#   - Redis availability
# so a single HTTP readiness probe is sufficient.
curl -sf --max-time 2 "http://localhost:${CPP_HTTP_PORT:-8088}/ready" >/dev/null
