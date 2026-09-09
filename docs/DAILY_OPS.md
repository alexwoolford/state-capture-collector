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

Collect reads `/var/lib/state-capture/announce/{db_name}.json` (`db_name`, `sqlite_path`). It does not ship a list of utilities. Never watch published `current/` copies (`VACUUM INTO` / `mv`).

Which work trees exist on a given host is **host inventory** (a systemd drop-in adding `ReadWritePaths` so drain can prune `_outbox` in each owner's file). That overlay lives in the private mosaic repo, not this crate. A probe whose sqlite already sits under `/var/lib/state-capture/` needs no extra path.

## Oracle (systemd)

SSH alias: `ct-firehose`. Build **on the box** (`aarch64-unknown-linux-gnu`).

```bash
cargo build --release
sudo ./deploy/install.sh
```

| Unit | Role |
|---|---|
| `state-capture-collect.socket` | `/run/state/collect.sock` (mode `0660`, group `state-capture`) |
| `state-capture-collect.service` | `--tick-secs 60` plus drain-on-nudge |

```bash
systemctl status state-capture-collect.service --no-pager
journalctl -u state-capture-collect.service -n 50 --no-pager
sudo /opt/state-capture-collector/bin/state-capture collect --once
ls /var/lib/state-capture/spool/
```

`capture.current` is incremental from when capture was enabled. After a feed break, or the first time a populated sqlite is captured, re-snapshot current rows into the spool (continues `_outbox` seq so later triggers cannot collide). Mini apply is unchanged:

```bash
sudo /opt/state-capture-collector/bin/state-capture collect --snapshot
# one db, seq floor when Mini watermark is already ahead of sqlite:
#   sudo ... collect --snapshot --db adsb-trip-journal --min-seq 10000004
```

Do not `UPDATE col=col` to storm triggers. Do not copy work sqlite to the Mini. Do not re-snapshot a large live table (FAA registry) as a refresh — that is how the spool and `capture.events` grow without bound ([Scale and retention](#scale-and-retention)).

Env: `/opt/state-capture-collector/etc/state-capture.env` (not overwritten on reinstall).

The datagram is group-scoped (`SocketGroup=state-capture`, mode `0660`). `install.sh` creates that group and adds each utility user that exists on the host (`faa`, `tails`, `adsb`, `entra`). A fifth utility needs the same group membership or the nudge gets `EACCES` (same as a missing socket: `_outbox` stays until the 60s tick). The service still runs as root so it can prune `_outbox` in each owner's work file. Postgres is **not** configured on this host. There is no `DATABASE_URL`.

Spool files: `/var/lib/state-capture/spool/{src_db}/{seq_lo}-{seq_hi}.jsonl` (tmp + fsync + rename). Mini rsyncs `*.jsonl` only.

The shipped unit allows write only under `/var/lib/state-capture`. A utility whose work sqlite lives elsewhere needs a host drop-in on `ReadWritePaths` (filesystem ACL so drain can prune `_outbox`, not a SQL schema). Do not add tile names to this crate's unit file.

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
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.woolford.state-capture-pull.plist
# already loaded: launchctl kickstart -k gui/$(id -u)/com.woolford.state-capture-pull
```

Default interval is 300s. `pull.sh` rsyncs JSONL, applies with `--delete-after`, then `ssh sudo forget-spool.sh` for paths that disappeared locally. Missing sudo fails the pull (no mtime fallback). See [Scale and retention](#scale-and-retention).

Idempotency: `PRIMARY KEY (src_db, seq)` on `capture.events` (`ON CONFLICT DO NOTHING`). Events already in the log are not re-applied to `current`. `I`/`U` refuse to recreate a row when `capture.events` already has a later `D` for that key. `list_jsonl` sorts by `(src_db, seq_lo)` parsed from `{lo}-{hi}.jsonl` (not path strings). Retract still runs after each file (`src_db`-scoped) to heal a current row that sits under a later `D`. `capture.current` upserts skip a row when the incoming `seq` is older than the stored one. Avoid overlapping `pull.sh` (launchd + manual) on the same incoming directory.

Hung Mini apply: hours in `INSERT INTO capture.current` with `wait_event=DataFileRead` and no later `apply complete`. Cause: each new `I`/`U` seq-scanned `capture.events` for a later `D`. Fix: `events_d_key` in `sql/001_capture.sql` (`apply --migrate`). A long `inserted=0` apply over hundreds of files is a one-time replay of JSONL still on Oracle; after `forget-spool.sh` that should not repeat.

## Postgres shape

Apply is generic. The collector does not know `entra` vs `adsb` vs a fifth name.

- `capture.events` — append-only log, `PRIMARY KEY (src_db, seq)`
- `capture.watermarks` — last applied `seq` per `src_db`
- `capture.current` — reconstructed live row (`src_db`, `tbl`, `key` JSONB → `after` JSONB)

`I`/`U` upsert `capture.current` from `after` (a `U` with `deleted_at` set still upserts). `D` deletes that key. Query the payload, not typed warehouse columns:

```sql
SELECT after->>'example_col'
FROM capture.current
WHERE src_db = 'example-utility' AND tbl = 'example_table';
```

Typed `adsb.trips`-style tables, if you want them later, are a separate Mini SQL/dbt layer on top of `capture.events`. They are not a reason to change this crate. If an earlier applyer created `entra` / `adsb` / `ttt` / `faa` schemas, drop those by hand; this binary no longer writes them.

A fifth utility: depend on [`capturable-state`](https://github.com/alexwoolford/capturable-state) (`tag = "v0.1.0"`), write announce JSON, nudge `/run/state/collect.sock`. Collector and applyer stay unchanged.

Envelope `ts` is Unix seconds. Fact dates inside `after` stay TEXT.

## Scale and retention

Low millions of JSONB rows are fine. `events_d_key` made new `I`/`U` cheap. Mini apply is O(JSONL still on Oracle). After a successful apply, `pull.sh` deletes those relative paths on Oracle (`forget-spool.sh`), so the next 300s pull is new files only.

| Store | Bound? | Notes |
|---|---|---|
| Work `_outbox` | Yes | Drain pages 5_000 rows (same as snapshot), then deletes `seq <= hi`. |
| Mini `incoming/` | Yes | `--delete-after` after each file commits. |
| `capture.current` | Yes | Live keys only. FAA-sized (~800k) at 10× is still a small warehouse table. |
| Oracle spool | Yes, after Mini apply | `/var/lib/state-capture/spool/{src_db}/{lo}-{hi}.jsonl` until `forget-spool.sh` removes the paths Mini just applied. |
| `capture.events` | **No** | Append-only CDC log. Years of ticker/ads-b/entra is modest. Repeating FAA `--snapshot` is not. |

Steady-state CDC (ads-b, ticker, entra) at 10× is noise next to one FAA snapshot. Do not `collect --snapshot` on `faa-registry-mirror` to refresh dictionaries.

**Do not** delete spool files because `hi <= capture.watermarks.last_seq` — watermark is max seq, not a contiguous fill. **Do not** `find -mtime +14 -delete` while Mini might be behind; `_outbox` is already gone and the JSONL is the remaining copy.

Oracle `forget-spool.sh` needs passwordless sudo for the SSH user (mosaic `deploy/ct-firehose/sudoers.d/state-capture-forget-spool`; on `ct-firehose` that user is `opc`). If sudo is missing, `pull.sh` exits non-zero. If the spool is already large, the next pull is a one-time replay; after forget, later pulls are new JSONL only.
