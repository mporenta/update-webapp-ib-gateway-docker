#!/usr/bin/env bash
set -euo pipefail

# 0) Notify the service to gracefully disconnect (ignore any errors)
curl --fail --show-error --silent \
  --request GET \
  'http://nyc.porenta.us/graceful-disconnect' \
  --header 'Accept: */*' || true
# 1) Pull & rebase the current repo from main
git stash push -u
git fetch origin
git checkout main
git pull --rebase origin main

# 2a) Stop & remove the old container that's using that image
docker compose -f docker-compose.yml stop ib-gateway || true
docker compose -f docker-compose.yml rm -f ib-gateway    || true

# 2b) Force‑remove the dangling image
docker image rm -f tbot-on-tradingboat-ib-gateway                   || true

# 3) Build & start just the two services
docker compose -f docker-compose.yml up -d --build ib-gateway nginx

# 4) Tail logs for the ib-gateway container
echo "Tailing logs for ib-gateway container..."
docker compose -f docker-compose.yml logs -f ib-gateway