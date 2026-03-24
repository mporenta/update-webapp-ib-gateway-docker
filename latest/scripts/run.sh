#!/bin/sh
set -eu

export DISPLAY=:1

rm -f /tmp/.X1-lock
# Optimized: smaller display (800x600), 8-bit color depth, no TCP listening
Xvfb :1 -ac -screen 0 800x600x8 -nolisten tcp &

if [ "${ENABLE_ZEROTIER:-false}" = "true" ] && [ -n "${ZEROTIER_NETWORK_ID:-}" ] && [ -x /usr/sbin/zerotier-one ]; then
  echo "Starting ZeroTier"
  /usr/sbin/zerotier-one -d
  sleep 5
  /usr/sbin/zerotier-cli join "${ZEROTIER_NETWORK_ID}" || true
fi

if [ -n "$VNC_SERVER_PASSWORD" ]; then
  echo "Starting VNC server"
  /root/scripts/run_x11_vnc.sh &
fi

envsubst < "${IBC_INI}.tmpl" > "${IBC_INI}"

/root/scripts/fork_ports_delayed.sh &

# Launch IB Gateway in background (was exec)
/root/ibc/scripts/ibcstart.sh "${TWS_MAJOR_VRSN}" -g \
     "--tws-path=${TWS_PATH}" \
     "--ibc-path=${IBC_PATH}" "--ibc-ini=${IBC_INI}" \
     "--user=${TWS_USERID}" "--pw=${TWS_PASSWORD}" "--mode=${TRADING_MODE}" \
     "--on2fatimeout=${TWOFA_TIMEOUT_ACTION}" &
IB_PID=$!

# Wait for IB Gateway process to appear
TIMEOUT=${CPP_WAIT_FOR_IB_SECONDS:-300}
elapsed=0
echo "Waiting for IB Gateway process..."
while [ $elapsed -lt $TIMEOUT ]; do
    if pgrep -f "ibgateway|jts" >/dev/null 2>&1; then
        echo "IB Gateway process detected after ${elapsed}s"
        break
    fi
    sleep 5
    elapsed=$((elapsed + 5))
done

# Launch C++ market data client (if binary exists)
CPP_PID=""
if [ -x /app/ib_market_data ]; then
    /app/ib_market_data &
    CPP_PID=$!
    echo "Started ib_market_data (PID=$CPP_PID)"
fi

# Supervisor loop
trap "kill $IB_PID $CPP_PID 2>/dev/null; wait $IB_PID 2>/dev/null; wait $CPP_PID 2>/dev/null; exit 0" SIGINT SIGTERM

while true; do
    if ! kill -0 $IB_PID 2>/dev/null; then
        echo "IB Gateway died, shutting down"
        [ -n "$CPP_PID" ] && kill $CPP_PID 2>/dev/null
        exit 1
    fi
    if [ -n "$CPP_PID" ] && ! kill -0 $CPP_PID 2>/dev/null; then
        echo "ib_market_data died, restarting..."
        /app/ib_market_data &
        CPP_PID=$!
    fi
    sleep 2
done
