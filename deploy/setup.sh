#!/usr/bin/env bash
# One-shot provisioning for a fresh Ubuntu 24.04 VM (Oracle free-tier ARM or
# anything comparable). Run as root:  sudo bash deploy/setup.sh
# Idempotent — safe to re-run.
set -euo pipefail

REPO="${SCOPE_REPO:-https://github.com/rvsiyad/scope.git}"
DIR=/opt/scope
GO_VERSION=1.27.0

apt-get update
apt-get install -y git curl docker.io docker-compose-v2
systemctl enable --now docker

# Go from go.dev — Ubuntu's packaged toolchain trails what go.mod asks for.
ARCH=$(dpkg --print-architecture) # arm64 on the free-tier shape, amd64 elsewhere
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go$GO_VERSION"; then
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" | tar -C /usr/local -xz
fi

# Dedicated system user; needs docker for the Ollama/Postgres containers.
id -u scope >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin scope
usermod -aG docker scope

[ -d "$DIR/.git" ] || git clone "$REPO" "$DIR"
chown -R scope:scope "$DIR"
cd "$DIR"

# Infra first (restart: unless-stopped keeps it across reboots), then the
# demo model — 1b fits a free-tier CPU — then the two binaries.
sudo -u scope docker compose up -d ollama ollama-backup postgres
sudo -u scope docker compose exec ollama ollama pull llama3.2:1b
sudo -u scope env HOME=/home/scope PATH="/usr/local/go/bin:$PATH" \
    go build -o bin/gateway ./cmd/gateway
sudo -u scope env HOME=/home/scope PATH="/usr/local/go/bin:$PATH" \
    go build -o bin/collector ./cmd/collector

install -m 644 deploy/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now scope-collector scope-gateway

# CD: let the deploy key run exactly one command as root, nothing else.
echo 'ubuntu ALL=(root) NOPASSWD: /opt/scope/deploy/redeploy.sh' > /etc/sudoers.d/scope-deploy
chmod 440 /etc/sudoers.d/scope-deploy

sleep 3
curl -fsS localhost:9091/healthz >/dev/null && echo "collector healthy"
curl -fsS localhost:8090/healthz >/dev/null && echo "gateway healthy"
echo "scope is up — gateway on :8090, dashboards on :9091/ui/ (open those ports in the cloud firewall)"
