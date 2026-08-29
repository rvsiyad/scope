#!/usr/bin/env bash
# What CD runs on every merge to main: pull, rebuild, restart, verify.
# Runs as root (via the sudoers entry setup.sh installs); build steps drop
# to the scope user.
set -euo pipefail
cd /opt/scope

sudo -u scope git fetch origin main
sudo -u scope git reset --hard origin/main
sudo -u scope docker compose up -d ollama ollama-backup postgres
sudo -u scope env HOME=/home/scope PATH="/usr/local/go/bin:$PATH" \
    go build -o bin/gateway ./cmd/gateway
sudo -u scope env HOME=/home/scope PATH="/usr/local/go/bin:$PATH" \
    go build -o bin/collector ./cmd/collector

systemctl restart scope-collector scope-gateway

sleep 3
# The collector must come back with its stores replayed and the gateway
# must see its providers; either failing fails the workflow, not just the VM.
curl -fsS localhost:9091/healthz >/dev/null
curl -fsS localhost:9091/ui/ >/dev/null
curl -fsS localhost:8090/healthz >/dev/null
echo "deployed $(git rev-parse --short HEAD)"
