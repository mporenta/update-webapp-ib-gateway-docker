#!/bin/bash

# Function to handle termination signals
function shutdown {
    echo "Received shutdown signal, terminating processes..."
    kill -s SIGTERM $RUN_PNL_MONITOR_PID
    kill -s SIGTERM $PNL_WEB_SERVICE_PID
    wait $RUN_PNL_MONITOR_PID
    wait $PNL_WEB_SERVICE_PID
    exit 0
}

# Trap termination signals
trap shutdown SIGTERM SIGINT

# Start run_pnl_monitor.py in the background
echo "Starting run_pnl_monitor.py..."
python /home/tbot/develop/github/portfolio-monitor/src/run_pnl_monitor.py &
RUN_PNL_MONITOR_PID=$!

# Wait for 30 seconds
echo "Waiting for 30 seconds..."
sleep 30

# Start pnl_web_service.py in the background
echo "Starting pnl_web_service.py..."
python /home/tbot/develop/github/portfolio-monitor/src/pnl_web_service.py &
PNL_WEB_SERVICE_PID=$!

# Wait for both processes to exit
wait $RUN_PNL_MONITOR_PID $PNL_WEB_SERVICE_PID
