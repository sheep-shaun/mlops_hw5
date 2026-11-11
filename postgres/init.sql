CREATE TABLE IF NOT EXISTS transaction_scores (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  transaction_id VARCHAR(128) NOT NULL,
  score DOUBLE PRECISION NOT NULL CHECK (
    score >= 0
    AND score <= 1
  ),
  fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
  model_version TEXT NOT NULL DEFAULT 'initial',
  scored_at TIMESTAMPTZ NOT NULL,
  us_state TEXT NOT NULL,
  merch TEXT NOT NULL,
  cat_id TEXT NOT NULL,
  amount DOUBLE PRECISION NOT NULL CHECK (amount >= 0 AND amount < 'Infinity'::float8),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS transaction_scores_fraud_recent ON transaction_scores (id DESC)
WHERE
  fraud_flag = 1;

CREATE TABLE IF NOT EXISTS labeled_transactions (
    source_key TEXT PRIMARY KEY,
    transaction_time TIMESTAMP NOT NULL,
    merch TEXT, cat_id TEXT, amount DOUBLE PRECISION,
    name_1 TEXT, name_2 TEXT, gender TEXT, street TEXT,
    one_city TEXT, us_state TEXT, post_code DOUBLE PRECISION,
    lat DOUBLE PRECISION, lon DOUBLE PRECISION, population_city BIGINT,
    jobs TEXT, merchant_lat DOUBLE PRECISION, merchant_lon DOUBLE PRECISION,
    target SMALLINT
);
CREATE INDEX IF NOT EXISTS labeled_month ON labeled_transactions (transaction_time);

CREATE TABLE IF NOT EXISTS training_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_month DATE NOT NULL,
    window_months INTEGER NOT NULL DEFAULT 3
);
CREATE TABLE IF NOT EXISTS training_runs (
    run_id TEXT PRIMARY KEY,
    month DATE NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'prepared',
    decision JSONB
);
CREATE TABLE IF NOT EXISTS supply_runs (
    run_id TEXT PRIMARY KEY,
    month DATE NOT NULL UNIQUE,
    rows_added INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS scores_time ON transaction_scores (scored_at);
CREATE INDEX IF NOT EXISTS scores_state_merch_time ON transaction_scores (us_state, merch, scored_at);

CREATE USER grafana_reader WITH PASSWORD 'grafana';
GRANT CONNECT ON DATABASE fraud TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON transaction_scores TO grafana_reader;
