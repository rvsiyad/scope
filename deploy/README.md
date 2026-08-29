# Deploying

The whole system runs on one small VM: Ollama (primary + failover backup)
and Postgres in Docker via the repo's compose file, the two Go binaries as
systemd units. Oracle Cloud's Always Free ARM shape (VM.Standard.A1.Flex,
up to 4 OCPU / 24 GB) fits comfortably — a 1b model on CPU is the whole
point of the $0 demo mode; any Ubuntu 24.04 box works.

## One-time setup

1. **Provision the VM** — Ubuntu 24.04, 2+ OCPU / 8+ GB recommended. In
   the cloud firewall / security list, open ingress TCP **8090** (the
   OpenAI-compatible gateway) and **9091** (dashboards + query API). Leave
   everything else closed — Ollama (11434/11435) and Postgres are only
   reached from localhost.
2. **Run the setup script** on the VM:

   ```
   sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/rvsiyad/scope/main/deploy/setup.sh)"
   ```

   or clone the repo and `sudo bash deploy/setup.sh`. It installs Go +
   Docker, creates the `scope` system user, clones to `/opt/scope`, brings
   up the containers (`restart: unless-stopped`, so they survive reboots),
   pulls the demo model, builds the two binaries, and installs + starts
   `scope-collector` and `scope-gateway`.
3. **Check**: `http://<vm>:9091/ui/` shows the live dashboards;
   `systemctl status 'scope-*'` shows the services. Point any OpenAI SDK
   at `http://<vm>:8090/v1` with the demo key from the gateway unit file
   and watch your own request appear in the request log.

The demo tenant's API key is public by design; its token budget
(60k tokens/minute) is the guardrail, and the budget dashboard is the
receipt. Edit `deploy/systemd/scope-gateway.service` to change tenants,
then `systemctl daemon-reload && systemctl restart scope-gateway`.

## Continuous deployment

`.github/workflows/deploy.yml` redeploys on every merge to `main` once the
repository is told where to deploy:

- **Repository variable** `DEPLOY_ENABLED` = `true` (Settings → Secrets and
  variables → Actions → Variables). Until it exists the job skips — CI
  stays green with no target configured.
- **Secrets**: `DEPLOY_HOST` (the VM's IP), `DEPLOY_USER` (e.g. `ubuntu`),
  `DEPLOY_SSH_KEY` (a private key whose public half is in the VM user's
  `~/.ssh/authorized_keys`; generate a dedicated pair with
  `ssh-keygen -t ed25519 -f deploy_key -N ""`).

The job SSHes in and runs `sudo /opt/scope/deploy/redeploy.sh` — the
sudoers entry installed by setup.sh allows exactly that one command —
which pulls `main`, rebuilds the binaries, restarts the units, and fails
the workflow unless the collector (stores replayed from the WAL), the
dashboard, and the gateway all come back healthy.

## Operations

```
journalctl -u scope-gateway -f          # gateway logs
journalctl -u scope-collector -f        # collector logs (WAL replay on boot)
du -sh /opt/scope/data/*                # what the stores are holding
curl -s localhost:9091/healthz | jq     # ingest counters, tsdb + trace status
curl -s localhost:8090/healthz | jq     # breakers, budgets, cache scoreboard
```

Retention is set in the collector unit (a week of metrics, three days of
traces); a restart is a WAL replay, which is to say: a crash demo. `kill
-9` the collector under load and watch `journalctl` show the replay put
every acknowledged sample back.
