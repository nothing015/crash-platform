# Part 4: Quality and monitoring (plan)

Owner: Rishabh Seth. Plan written on 2026-11-16 for the final report due 2026-11-26.

Part 4 has two jobs:

- **Quality:** prove that the data and the answers to the three questions can be trusted. Write down the rules the data must follow, check them on every run, and say what happens to rows that break them.
- **Monitoring:** show how healthy the pipeline is while it runs: how many rows go in and out of each part, how long each part takes, how far behind the stream is, how much memory each container uses and how fresh the database is.

It also covers the proposal's second risk, that four parts built separately do not connect. The plan there was one end-to-end test of the whole pipeline every week.

Check this plan against the course's Part 4 requirements before starting, and drop or add steps to match them.

## 1. What already exists

Parts 1 to 3 already check and measure a lot. Part 4 should reuse this, not rebuild it.

| Where | What it does | Output |
| --- | --- | --- |
| `jobs/common.py`, `write_metrics` | Counts rows and nulls after reading the CSV | `results/metrics_part1.json` |
| `jobs/check_part1.py` | 8 PASS/FAIL checks on Part 1 (row count, files, sizes, Q1 and Q2 shapes), plus 4 on the experiments | Exit code 1 on failure |
| `jobs/queries.py` | Recomputes Q1 and Q2 from the raw CSV and compares them with the Parquet answers | `Q1 Parquet matches CSV: True` |
| `stream/producer.py` | Counts rows read, rows dropped for a missing timestamp, rows sent, delivered and failed; peak memory | `results/stream_producer_metrics.json` |
| `stream/consumer.py` | Counts bad messages and UNKNOWN boroughs; measures lag, throughput and peak memory | `results/stream_consumer_metrics.json` |
| `stream/check_stream.py` | 9 checks, including a full recount of Q3 from Parquet | Exit code 1 on failure |
| `scripts/run_restart_demo.sh` | Kills the consumer mid-replay and checks that nothing is lost or repeated | 9 checks |
| `serve/schema.sql` | Rejects negative counts, duplicate keys and hours outside 0 to 23 | Load rolls back |
| `serve/check_serve.py` | 10 checks: every table equals its file, and the latest hour agrees with the hourly table | Exit code 1 on failure |
| `load_log` table | When each table was last loaded, how many rows and how long it took | Postgres |
| `results/data_info.txt` | Size and SHA-256 checksum of the downloaded CSV | Text file |

The latest numbers (download of 2026-11-15, in `results/metrics_part1.json`):

| Measure | Value |
| --- | --- |
| Rows | 2,305,623 |
| Rows with no crash timestamp | 0 |
| Rows with no borough | 674,935 (29.3%) |
| Rows with no on-street name | 510,899 (22.2%) |

## 2. What is missing

1. **No written data rules.** The checks test the outputs (10 streets, 24 hours) but not the input data itself: valid ranges, allowed values, duplicates.
2. **Bad values disappear quietly.** `try_to_timestamp` and `try_cast` turn bad values into nulls. Nobody counts how many values were bad, as opposed to missing in the source.
3. **Street names are not standardised.** Q1 groups by the raw name, so different spellings of the same road are counted separately. A quick search of the raw CSV (any street column, so only a rough guide to on-street counts) finds:

   | Spelling | Rows |
   | --- | --- |
   | `GRAND CENTRAL PKWY` (8th in Q1) | 12,278 |
   | `GRAND CENTRAL PARKWAY` | 5,821 |
   | `BROOKLYN QUEENS EXPRESSWAY` (5th in Q1) | 14,835 |
   | `BROOKLYN QUEENS EXPY` | 138 |
   | `3 AVENUE` (6th in Q1) | 31,433 |
   | `THIRD AVENUE` | 551 |
   | `3RD AVENUE` | 4 |

   If most of the `GRAND CENTRAL PARKWAY` rows are on-street names, joining the two spellings gives about 18,000 crashes. That would move Grand Central from 8th to 3rd, ahead of Atlantic Avenue (17,906). So this can change the answer to Q1, and it matters most of the gaps listed here.
4. **No check across parts.** Each part checks itself. Nothing checks that all parts used the same download, or that the row counts add up from one part to the next.
5. **Metrics are scattered.** They sit in four JSON files and one table, each overwritten by the next run. There is no history and no single place to look.
6. **No thresholds or alerts.** For example, nothing says how much consumer lag is too much or how old the database can get.
7. **Two small numbers to explain.** The producer dropped 1 row with no timestamp from `sample_data/`, but Part 1 reports 0 such rows for the full data. The sample was probably cut from an older download, so check that. And `docs/part2_stream.md` quotes a different download (2,303,506 rows) from the current one (2,305,623).

## 3. Steps

### Step 1: Write down the data rules

Agree on the rules with the team first, since they decide what counts as a correct answer. Give each rule a level: **fail** stops the pipeline, **warn** is reported but lets the run carry on.

| # | Rule | Level | Why |
| --- | --- | --- | --- |
| R1 | The CSV has every column the jobs use, with the expected names | fail | A renamed column breaks every part |
| R2 | Row count is at least 1,000,000 and within 5% of the previous run | fail / warn | Catches a truncated download, or a big change in the source |
| R3 | `crash_ts` parses for at least 99.9% of rows | fail | Event time drives Q2, Q3 and the replay |
| R4 | `crash_ts` is between 2012-07-01 and the download date | warn | Catches typos and future dates |
| R5 | `borough` is one of the five boroughs, or empty | fail | Q3 groups by borough |
| R6 | Injured and killed counts are whole numbers of at least 0 | fail | Q2 adds them up |
| R7 | Count of values that `try_cast` turned into null, per column | warn | Separates bad values from missing ones |
| R8 | `collision_id` is unique | warn | A duplicate row is counted twice in every question |
| R9 | Latitude and longitude are inside NYC (about 40.49 to 40.92 N, 74.26 to 73.70 W), or empty or 0 | warn | Shows how much location data is usable |
| R10 | Share of empty boroughs and empty street names stays near the current 29% and 22% | warn | A jump means the source changed |
| R11 | Street names map to a standard form (`PKWY` to `PARKWAY` and so on) before grouping for Q1 | warn | See point 3 in section 2 |

### Step 2: Build the quality job (Spark)

Write `jobs/quality.py`. It reuses `get_cleaned_crashes()` from `jobs/common.py` and runs rules R1 to R10 in one pass over the Parquet files, so it stays fast.

- Output `results/quality_report.json`: one entry per rule with the rule, level, measured value, threshold and result (pass, warn or fail).
- Print the same PASS/WARN/FAIL lines as the other check scripts, and exit with code 1 if any fail rule fails.
- Run it from `scripts/run_part1.sh` straight after the conversion, so a bad download stops before the questions are answered.
- Keep the bad rows out of the answers, but put them somewhere they can be looked at: write up to 1,000 of them to `data/quarantine/`, with the name of the rule each one broke.

For R11, compare Q1 with and without standard street names. Show both tables in the report and say whether the ranking changes. Only change `jobs/queries.py` once the team agrees, because it changes the committed answer.

### Step 3: Add stream quality checks

The consumer already skips and counts bad messages. Add the following:

- **Fault injection:** a small script, `stream/inject_bad.py`, that sends a few broken messages into the topic: not JSON, a bad timestamp, a number as the borough, a duplicate `collision_id`. The run must report exactly that many bad messages, and Q3 must still equal the batch recount. This tests the "bad input" design in `docs/part2_stream.md`.
- **Late and duplicate rows:** count rows that arrive for an hour older than the latest hour, and rows whose `collision_id` was already seen. Report both in `stream_consumer_metrics.json`.
- **Lag over time:** the consumer already measures lag. Have it append `time, offset, lag, messages_per_second` to `results/stream_lag.csv` every few seconds, so the report can show a chart instead of one end value.

### Step 4: Check across parts (end-to-end)

Write `scripts/run_all.sh`, which runs `start.sh`, `run_stream_demo.sh` and `run_serve.sh` in order, then a new `scripts/check_pipeline.py`. That script checks:

| Check | How |
| --- | --- |
| Every part used the same download | Checksum in `results/data_info.txt`, recorded next to each part's metrics |
| Rows add up from Part 1 to Part 2 | For a full run: producer `rows_read` equals Part 1 `total_rows`, and `rows_read - rows_dropped_null_ts = rows_sent` |
| The stream answer matches the batch answer | Already in `check_stream.py`; report its result here |
| The database matches the files | Already in `check_serve.py`; report its result here |
| Q2 adds up | The sum of `injured` in `q2_injuries_by_hour` equals the sum of `number_of_persons_injured` over rows with a timestamp |
| Q3 adds up | The sum of `crashes` in the hourly table equals the messages the consumer read |
| The answers are fresh | The last `load_log` time is later than the last stream run and the last Part 1 run |

Run `./scripts/run_all.sh` once a week (the proposal's plan) and before recording the video.

### Step 5: Collect the metrics in one place

Add two tables to the Postgres database from Part 3 rather than a new tool. It is already running and stays within the memory plan.

```sql
CREATE TABLE pipeline_runs (
    run_id       text,
    part         text,          -- part1, stream_producer, stream_consumer, serve
    started_at   timestamptz,
    seconds      real,
    rows_in      bigint,
    rows_out     bigint,
    peak_mem_mb  real,
    status       text,          -- pass, warn or fail
    details      jsonb          -- the full metrics JSON
);

CREATE TABLE quality_results (
    run_id     text,
    rule       text,
    level      text,
    value      double precision,
    threshold  double precision,
    result     text
);
```

Extend `serve/load.py`, or add `serve/load_metrics.py`, to read the metrics JSON files and `quality_report.json` after each run and add rows. Each run gets a new row instead of overwriting the last, so the report can compare runs over time. Record container memory as well, with `docker stats --no-stream` called from `run_all.sh`.

### Step 6: Show the monitoring

Pick one option:

- **Simple (recommended for the time left):** `serve/monitor.py` prints a status page from the two tables: the last run of each part, pass/warn/fail counts, consumer lag, database age and peak memory. Charts for the report can come from the same queries (matplotlib, or the VS Code PostgreSQL extension).
- **Dashboard:** add a Grafana container to `docker-compose.yml`, with a `mem_limit` of about 256 MB, connected to Postgres. It looks better in the video, but it adds a container and time to set up. Measure memory with everything running before deciding, because the proposal's first risk is fitting on one 8 GB laptop.

### Step 7: Set thresholds and alerts

Write the thresholds down and have `check_pipeline.py` or `monitor.py` report a breach as WARN or FAIL:

| Signal | Warn | Fail |
| --- | --- | --- |
| Consumer lag at the end of a run | over 0 | over 1,000 messages |
| Database age | 1 day | 7 days |
| Container memory | 80% of its `mem_limit` | out-of-memory stop |
| Bad stream messages | over 0 | over 1% of messages |
| Row count change from the previous run | over 2% | over 5% |

An "alert" here can just be a FAIL line and exit code 1. That is honest for a laptop demo. Say in the report what a real deployment would add, such as email or a pager.

### Step 8: Document and demo

- Write `docs/part4_quality_monitoring.md` as a results section, in the same style as `docs/part2_stream.md` and `docs/part3_serve.md`: the rules and their results, the data notes, the monitoring screen and the fault tests, each with what was found.
- Add Part 4 to the README: `./scripts/run_all.sh` in Quick start, its output files under "What you get", and the new files under "Project layout".
- For the video, run `./scripts/run_all.sh`, show the quality report, inject bad messages, kill the consumer, and show that the checks and the monitor catch all of it.

## 4. Timeline

| Dates | Work |
| --- | --- |
| Nov 16 to 17 | Agree the rules (Step 1) with the team; look into the street names (R11) and the 1 dropped sample row |
| Nov 18 to 20 | Quality job (Step 2) and stream checks (Step 3) |
| Nov 21 to 22 | End-to-end script (Step 4) and metrics tables (Step 5) |
| Nov 23 to 24 | Monitoring (Step 6), thresholds (Step 7), full run on every row |
| Nov 25 | Write-up, README and video recording (Step 8) |
| Nov 26 | Report due |

## 5. Checklist

- [ ] Rules R1 to R11 agreed with the team
- [ ] `jobs/quality.py` writes `results/quality_report.json` and is called from `scripts/run_part1.sh`
- [ ] Bad rows written to `data/quarantine/` with the rule they broke
- [ ] Q1 compared with and without standard street names; decision recorded
- [ ] `stream/inject_bad.py` and its result in the checks
- [ ] Late and duplicate counts and `results/stream_lag.csv` from the consumer
- [ ] `scripts/run_all.sh` and `scripts/check_pipeline.py`
- [ ] `pipeline_runs` and `quality_results` tables filled after each run
- [ ] Monitor screen or dashboard
- [ ] Thresholds written down and enforced
- [ ] Results section, README updated, video recorded
