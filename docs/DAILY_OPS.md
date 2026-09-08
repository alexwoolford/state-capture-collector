# State capture collector (ops)

Two agents, one binary (`state-capture`). Oracle never connects to Postgres on the Mini.

| Agent | Host | Process | Daemon? |
|---|---|---|---|
| `state-capture collect` | `ct-firehose` (Oracle Linux) | systemd `Type=simple` + socket | Yes. Full-time. Drain on datagram wake **or** 60s tick |
| `scripts/pull.sh` then `state-capture apply` | Mac Mini | launchd `StartInterval` 300s | No resident applyer. Script rsyncs closed JSONL, apply exits |

Collect writes closed JSONL under `/var/lib/state-capture/spool`. The Mini **grabs** those files; it does not open work sqlite. Missing `/run/state/collect.sock` is ignored by utilities (`ECONNREFUSED`). `_outbox` is the source of truth until drain.

The Unix datagram `/run/state/collect.sock` is a **wake**. Payload is `db_name` bytes, not a row. Announce files are `/var/lib/state-capture/announce/{db_name}.json` (`db_name`, `sqlite_path`). A fifth utility depends on [`capturable-state`](https://github.com/alexwoolford/capturable-state) at a git tag, writes announce JSON, and nudges the same socket. It does not get its own collector.

## Scheduler and telemetry

`collect` is a `Type=simple` daemon (socket activation + 60s tick). `apply` is launchd `StartInterval` 300s, not a resident process. Do not put a cron inside the binary.

Operator logs: `tracing` on stderr → journald / launchd. Default `RUST_LOG=info`. The 60s tick is a freshness fallback, not a job scheduler for utilities.

## Watch work sqlite only

| Logical name | Watched path |
|---|---|
| `entra-tenant-recon` | `/var/lib/entra-tenant-recon/entra.sqlite` |
| `adsb-trip-journal` | `/var/lib/adsb-trip-journal/trips.sqlite` |
| `tail-to-ticker` | `/var/lib/tail-to-ticker/work/current/tail_to_ticker.sqlite` |
| `faa-registry-mirror` | `/var/lib/faa-registry-mirror/work/faa-registry.sqlite` |

Never watch published `current/` copies (`VACUUM INTO` / `mv`).

## Oracle (systemd)

SSH alias: `ct-firehose`. Build **on the box** (`aarch64-unknown-linux-gnu`).

```bash
cargo build --release
sudo ./deploy/install.sh
```

| Unit | Role |
|---|---|
| `state-capture-collect.socket` | `/run/state/collect.sock` (mode 666, local datagram) |
| `state-capture-collect.service` | `--tick-secs 60` plus drain-on-nudge |

```bash
systemctl status state-capture-collect.service --no-pager
journalctl -u state-capture-collect.service -n 50 --no-pager
sudo /opt/state-capture-collector/bin/state-capture collect --once
ls /var/lib/state-capture/spool/
```

Env: `/opt/state-capture-collector/etc/state-capture.env` (not overwritten on reinstall).

The service runs as root so it can prune `_outbox` in each owner's work file. Postgres is **not** configured on this host. There is no `DATABASE_URL`.

Spool files: `/var/lib/state-capture/spool/{src_db}/{seq_lo}-{seq_hi}.jsonl` (tmp + fsync + rename). Mini rsyncs `*.jsonl` only.

`ReadWritePaths` in the systemd unit is a filesystem ACL (so drain can prune `_outbox`), not a SQL schema. A fifth utility under a new `/var/lib/...` needs one unit line. A probe under `/var/lib/state-capture/` needs none.

## Mini (launchd)

Local Postgres **database** `mosaic` (`localhost` only). Schema `capture` inside it is the generic transport (`events`, `watermarks`, `current`). Create the database, then migrate on first apply (`--migrate` is idempotent):

```bash
createdb mosaic
export DATABASE_URL=postgres://$(whoami)@localhost/mosaic
export ORACLE_SSH=ct-firehose   # Host in ~/.ssh/config
cargo build --release
./scripts/pull.sh
```

Edit [deploy/macos/com.woolford.state-capture-pull.plist](../deploy/macos/com.woolford.state-capture-pull.plist) (`ORACLE_SSH`, `DATABASE_URL`, path to `pull.sh`), then:

```bash
cp deploy/macos/com.woolford.state-capture-pull.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.woolford.state-capture-pull.plist
```

Default interval is 300s. `pull.sh` deletes **local** copies after a successful apply (`--delete-after`). Remote spool is left for retention; prune by hand if the disk grows.

Idempotency: `PRIMARY KEY (src_db, seq)` on `capture.events` (`ON CONFLICT DO NOTHING`). `capture.current` upserts skip a row when the incoming `seq` is older than the stored one. Re-pulling the same JSONL is a no-op for the log.

## Postgres shape

Apply is generic. The collector does not know `entra` vs `adsb` vs a fifth name.

- `capture.events` — append-only log, `PRIMARY KEY (src_db, seq)`
- `capture.watermarks` — last applied `seq` per `src_db`
- `capture.current` — reconstructed live row (`src_db`, `tbl`, `key` JSONB → `after` JSONB)

`I`/`U` upsert `capture.current` from `after` (a `U` with `deleted_at` set still upserts). `D` deletes that key. Query the payload, not typed warehouse columns:

```sql
SELECT after->>'ticker'
FROM capture.current
WHERE src_db = 'tail-to-ticker' AND tbl = 'mappings_current';
```

Typed `adsb.trips`-style tables, if you want them later, are a separate Mini SQL/dbt layer on top of `capture.events`. They are not a reason to change this crate. If an earlier applyer created `entra` / `adsb` / `ttt` / `faa` schemas, drop those by hand; this binary no longer writes them.

A fifth utility: depend on [`capturable-state`](https://github.com/alexwoolford/capturable-state) (`tag = "v0.1.0"`), write announce JSON, nudge `/run/state/collect.sock`. Collector and applyer stay unchanged.

Envelope `ts` is Unix seconds. Fact dates inside `after` stay TEXT.
