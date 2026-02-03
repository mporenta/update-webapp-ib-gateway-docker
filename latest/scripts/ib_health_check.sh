#!/bin/bash
# IB Gateway Health Check Script
# Runs larry_ib_async.py and checks if NVDA bars are returned
# Exit 0 = healthy, Exit 1 = unhealthy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="${SCRIPT_DIR}/larry_ib_async.py"

# Run the Python script and capture output
OUTPUT=$(/opt/venv/bin/python "$PYTHON_SCRIPT" 2>&1)
EXIT_CODE=$?

# Check if bars were received (look for BarData in output)
if echo "$OUTPUT" | grep -q "BarData"; then
    # Bars received - healthy
    exit 0
else
    # No bars or error - unhealthy
    echo "Health check failed: No NVDA bars received"
    echo "$OUTPUT"
    exit 1
fi
