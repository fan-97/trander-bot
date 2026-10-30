-- Blocks between "-- timescale:begin" and "-- timescale:end" are stripped only
-- by the integration test harness (TB_SKIP_TIMESCALE=1 in TestMain) so the
-- schema can be exercised on plain PostgreSQL. Production always applies them.

-- timescale:begin
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- timescale:end

CREATE TABLE klines (
    symbol       text             NOT NULL,
    interval     text             NOT NULL,
    open_time    timestamptz      NOT NULL,
    open         double precision NOT NULL,
    high         double precision NOT NULL,
    low          double precision NOT NULL,
    close        double precision NOT NULL,
    volume       double precision NOT NULL,
    quote_volume double precision NOT NULL,
    PRIMARY KEY (symbol, interval, open_time)
);

CREATE TABLE mark_prices (
    symbol       text             NOT NULL,
    time         timestamptz      NOT NULL,
    mark_price   double precision NOT NULL,
    funding_rate double precision NOT NULL,
    PRIMARY KEY (symbol, time)
);

CREATE TABLE open_interest (
    symbol text             NOT NULL,
    time   timestamptz      NOT NULL,
    oi     double precision NOT NULL,
    PRIMARY KEY (symbol, time)
);

CREATE TABLE long_short (
    symbol text             NOT NULL,
    time   timestamptz      NOT NULL,
    kind   text             NOT NULL,
    ratio  double precision NOT NULL,
    PRIMARY KEY (symbol, kind, time)
);

-- Liquidations have no natural key: two identical prints are both real.
CREATE TABLE liquidations (
    symbol text             NOT NULL,
    time   timestamptz      NOT NULL,
    side   text             NOT NULL,
    price  double precision NOT NULL,
    qty    double precision NOT NULL
);
CREATE INDEX liquidations_symbol_time_idx ON liquidations (symbol, time DESC);

CREATE TABLE signals (
    id         bigserial        NOT NULL,
    time       timestamptz      NOT NULL,
    rule_id    text             NOT NULL,
    symbol     text             NOT NULL,
    interval   text             NOT NULL,
    level      text             NOT NULL,
    price      double precision NOT NULL,
    detail     text             NOT NULL,
    channels   text[]           NOT NULL,
    suppressed text             NOT NULL,
    PRIMARY KEY (id, time)
);
CREATE INDEX signals_time_idx ON signals (time);
CREATE INDEX signals_rule_id_time_idx ON signals (rule_id, time);

-- timescale:begin
-- No default time indexes: every PK already contains the time
-- column, and liquidations has its own (symbol, time DESC) index, so the
-- index set matches the plain-Postgres test schema exactly.
SELECT create_hypertable('klines', 'open_time', if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('mark_prices', 'time', if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('open_interest', 'time', if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('long_short', 'time', if_not_exists => TRUE, create_default_indexes => FALSE);
SELECT create_hypertable('liquidations', 'time', if_not_exists => TRUE, create_default_indexes => FALSE);

ALTER TABLE klines SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, interval',
    timescaledb.compress_orderby = 'open_time DESC'
);
SELECT add_compression_policy('klines', INTERVAL '7 days');
-- timescale:end
