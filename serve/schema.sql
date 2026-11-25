-- Part 3 tables. One table per result file from Part 1 (questions 1 and 2) and Part 2 (question 3).
-- Hours are New York local time, as in Parts 1 and 2 (see docs/part2_stream.md, "Time zone").

-- Question 1: the ten streets with the most crashes (results/q1_top_streets.csv)
CREATE TABLE IF NOT EXISTS q1_top_streets (
    street   text    PRIMARY KEY,
    crashes  integer NOT NULL CHECK (crashes >= 0)
);

-- Question 2: people injured per hour of day (results/q2_injuries_by_hour.csv)
CREATE TABLE IF NOT EXISTS q2_injuries_by_hour (
    hour_of_day  smallint PRIMARY KEY CHECK (hour_of_day BETWEEN 0 AND 23),
    injured      integer  NOT NULL CHECK (injured >= 0)
);

-- Question 3: crashes for every hour and borough (results/q3_crashes_per_borough_hour.csv)
CREATE TABLE IF NOT EXISTS q3_crashes_per_borough_hour (
    hour     timestamp NOT NULL,
    borough  text      NOT NULL,
    crashes  integer   NOT NULL CHECK (crashes >= 0),
    PRIMARY KEY (hour, borough)
);

-- Question 3: the answer, one row per borough for the latest hour (results/q3_latest_hour.csv)
CREATE TABLE IF NOT EXISTS q3_latest_hour (
    hour     timestamp NOT NULL,
    borough  text      PRIMARY KEY,
    crashes  integer   NOT NULL CHECK (crashes >= 0)
);

-- One row per table per load, so you can see when each answer was last refreshed
CREATE TABLE IF NOT EXISTS load_log (
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    table_name   text        NOT NULL,
    source_file  text        NOT NULL,
    rows_loaded  integer     NOT NULL,
    seconds      real        NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       text        NOT NULL,
    part         text        NOT NULL,
    started_at   timestamptz,
    seconds      real,
    rows_in      bigint,
    rows_out     bigint,
    peak_mem_mb  real,
    status       text        NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, part)
);

CREATE TABLE IF NOT EXISTS quality_results (
    run_id     text             NOT NULL,
    rule       text             NOT NULL,
    level      text             NOT NULL,
    value      double precision,
    threshold  double precision,
    result     text             NOT NULL,
    details    jsonb            NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, rule)
);
