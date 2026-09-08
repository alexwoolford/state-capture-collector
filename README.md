# state-capture

Drain SQLite `_outbox` tables on the Oracle host (`ct-firehose`) into closed JSONL spool files. On the Mac Mini, pull those files over SSH and apply them to **local** Postgres database `mosaic` (`capture.events` plus generic `capture.current` JSONB). The applyer has no per-utility schema. The cloud VM never opens a connection to the Mini.

Same binary, different subcommands: `collect` is a full-time systemd daemon; `apply` runs from a 300s launchd timer via `scripts/pull.sh` and exits.

See [docs/DAILY_OPS.md](docs/DAILY_OPS.md). Utilities join this bus by depending on [`capturable-state`](https://github.com/alexwoolford/capturable-state) (git tag). This crate stays schema-ignorant.

```bash
# Oracle (ct-firehose) — daemon
state-capture collect --once
state-capture collect              # socket + 60s tick

# Mini — periodic grab into database mosaic
state-capture apply --migrate --spool ./incoming --database-url postgres://localhost/mosaic
```
