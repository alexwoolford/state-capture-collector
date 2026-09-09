CREATE SCHEMA IF NOT EXISTS capture;

CREATE TABLE IF NOT EXISTS capture.events (
  src_db TEXT NOT NULL,
  seq    BIGINT NOT NULL,
  tbl    TEXT NOT NULL,
  op     CHAR(1) NOT NULL CHECK (op IN ('I','U','D')),
  ts     BIGINT NOT NULL,
  key    JSONB NOT NULL,
  before JSONB,
  after  JSONB,
  PRIMARY KEY (src_db, seq)
);

CREATE TABLE IF NOT EXISTS capture.watermarks (
  src_db TEXT PRIMARY KEY,
  last_seq BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS events_tbl ON capture.events (src_db, tbl);
-- Later-D lookup for apply_current / retract_stale_current. Without this,
-- each I/U seq-scans capture.events (~250ms, ~1.4M rows) and Mini hangs.
CREATE INDEX IF NOT EXISTS events_d_key ON capture.events (src_db, tbl, key, seq)
  WHERE op = 'D';

CREATE TABLE IF NOT EXISTS capture.current (
  src_db TEXT NOT NULL,
  tbl    TEXT NOT NULL,
  key    JSONB NOT NULL,
  after  JSONB,
  seq    BIGINT NOT NULL,
  ts     BIGINT NOT NULL,
  PRIMARY KEY (src_db, tbl, key)
);

CREATE INDEX IF NOT EXISTS current_tbl ON capture.current (src_db, tbl);
