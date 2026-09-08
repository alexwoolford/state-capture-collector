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
