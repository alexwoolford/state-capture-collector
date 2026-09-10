# state-capture

Drain SQLite `_outbox` tables on the Oracle host (`ct-firehose`) into closed JSONL spool files. On the Mac Mini, pull those files over SSH and apply them to **local** Postgres database `mosaic` (`capture.events` plus generic `capture.current` JSONB). The applyer has no per-utility schema. The cloud VM never opens a connection to the Mini.

Same binary, different subcommands: `collect` is a full-time systemd daemon; `apply` runs from a 300s launchd timer via `scripts/pull.sh` and exits.

See [docs/DAILY_OPS.md](docs/DAILY_OPS.md) (including [Scale and retention](docs/DAILY_OPS.md#scale-and-retention)). Utilities join this bus by depending on [`capturable-state`](https://github.com/alexwoolford/capturable-state) (git tag). This crate stays schema-ignorant: it assumes `_outbox(seq, tbl, op, ts, key, before, after)`, `_cap_I_{table}` triggers, and announce `{db_name, sqlite_path}`.

```bash
# Oracle (ct-firehose) — daemon
state-capture collect --once
state-capture collect              # socket + 60s tick (--tick-secs on the unit)

# After a feed break: write-lock that sqlite, drain _outbox, emit I events
# state-capture collect --snapshot
# state-capture collect --snapshot --db adsb-trip-journal --min-seq 10000004

# Mini — periodic grab into database mosaic
state-capture apply --migrate --spool ./incoming --database-url postgres://localhost/mosaic
```
