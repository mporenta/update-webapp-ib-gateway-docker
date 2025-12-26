#!/bin/sh

export DISPLAY=:1

rm -f /tmp/.X1-lock
# Optimized: smaller display (800x600), 8-bit color depth, no TCP listening
Xvfb :1 -ac -screen 0 800x600x8 -nolisten tcp &

if [ -n "$VNC_SERVER_PASSWORD" ]; then
  echo "Starting VNC server"
  /root/scripts/run_x11_vnc.sh &
fi

envsubst < "${IBC_INI}.tmpl" > "${IBC_INI}"

/root/scripts/fork_ports_delayed.sh &

/root/ibc/scripts/ibcstart.sh "${TWS_MAJOR_VRSN}" -g \
     "--tws-path=${TWS_PATH}" \
     "--ibc-path=${IBC_PATH}" "--ibc-ini=${IBC_INI}" \
     "--user=${TWS_USERID}" "--pw=${TWS_PASSWORD}" "--mode=${TRADING_MODE}" \
     "--on2fatimeout=${TWOFA_TIMEOUT_ACTION}"
