#!/bin/sh

sleep 30

# Kill any existing socat forwarders to avoid "Address already in use"
pkill -f "socat.*TCP-LISTEN:400" 2>/dev/null || true
sleep 1

if [ "$TRADING_MODE" = "paper" ]; then
  printf "Forking :::4000 onto 0.0.0.0:4002\n"
  socat TCP-LISTEN:4002,fork,reuseaddr TCP:127.0.0.1:4000
else
  printf "Forking :::4000 onto 0.0.0.0:4001\n"
  socat TCP-LISTEN:4001,fork,reuseaddr TCP:127.0.0.1:4000
fi
