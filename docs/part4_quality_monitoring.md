# Part 4: Quality and monitoring

Part 4 validates the source data, retains rejected records for inspection, measures
rows and durations across pipeline stages, checks that Parts 1–3 agree, and provides a
local monitoring dashboard.

## Run it

Run the normal end-to-end demo (Part 2 streams 300 rows from the current validated
dataset):

```sh
./scripts/run_all.sh
```

To stream every accepted row instead:

```sh
FULL_STREAM=1 ./scripts/run_all.sh
```

The full-stream mode takes several minutes and runs the same Part 1, stream and database
checks. `run_all.sh` also runs the stream fault-injection check and records container
memory snapshots. Run it weekly and before recording the project demo.

After a run, start the dashboard in another terminal:

```sh
./scripts/run_dashboard.sh
```

Open <http://localhost:8050>. The page refreshes every 15 seconds and has a manual
refresh button.

## Data rules and treatment

`jobs/quality.py` evaluates the source data and records a failed-row count and threshold
for each rule. A row failing one or more row-level rules goes to the quarantine dataset;
it is excluded from validated analytics. Multiple rule failures on one record are
recorded together, so rule counts can overlap.

| Rule | Level | Failure threshold | Treatment |
| --- | --- | --- | --- |
| Required pipeline columns exist | Fail | 0 rows | Report schema mismatch; pipeline cannot process missing required fields |
| Crash date and time parse | Fail | 0.1% | Quarantine unparseable timestamps |
| Timestamp is between 2012-07-01 and the download date | Warn | 1% | Quarantine out-of-range timestamps and report threshold breaches |
| Injury and fatality counts are nonnegative whole numbers | Fail | 0.1% | Quarantine invalid values |
| Non-empty injury/fatality values cast to integers | Warn | Any | Report invalid casts by column; quarantine affected records |
| Borough is one of the five boroughs or empty | Fail | 0.1% | Quarantine unknown borough values; empty source values are allowed |
| Collision ID is present | Fail | 0 rows | Quarantine rows without an ID |
| Collision ID is unique | Warn | 1% | Quarantine duplicate IDs |
| Coordinates are within NYC bounds, empty, or zero | Warn | 5% | Quarantine out-of-range coordinates and report threshold breaches |

Separate run-level checks require at least 1,000,000 input rows, report changes greater
than 2% as warnings and greater than 5% as failures, and compare missing borough/street
shares with baselines (29.3% and 22.2%, with a two-percentage-point warning margin).
Missing values are reported separately from failed casts.

Threshold breaches appear as PASS, ALERT, or FAIL in the report and dashboard; they do
not, by themselves, stop Part 1. Structural problems that prevent processing and
failures in the regular pipeline/checkers can still stop a run. Rejected rows are never
silently discarded.

## Latest full-data results

The full run used the 2026-11-24 download (SHA-256 recorded in
`results/data_info.txt`) and completed on 2026-11-25.

| Measure | Result |
| --- | ---: |
| Raw source rows | 2,307,963 |
| Accepted / quarantined | 2,307,813 / 150 |
| Quality rules | 9 passed; 0 alerts; 0 failures |
| Coordinate rule | 150 failed rows, below its 5% threshold |
| Invalid count casts | 0 in all eight injury/fatality columns |
| Missing borough / on-street name | 675,014 (29.25%) / 511,634 (22.17%) |
| Full-stream read / sent / consumed | 2,307,813 / 2,307,813 / 2,307,813 |
| Bad / duplicate / late stream messages | 0 / 0 / 0 |
| Ending stream lag | 0 |
| Q2 injury total | 780,859 |
| Q3 hourly crashes | 2,307,813 |
| Cross-part checks | 8/8 passed |

The producer took about 200 seconds and the consumer about 211 seconds. Spark's measured
container peak was about 3,015 MB of its 4,096 MB limit; the producer peaked at about
1,152 MB of 2,048 MB, and the consumer at about 932 MB of 2,048 MB. The latest sampled
Kafka and database memory use was about 43% of 1 GiB and 17% of 512 MiB respectively.
Stage-level durations and memory peaks are recorded in `results/stage_metrics.json`;
container snapshots are in `results/container_memory.csv`.

The row-count invariant at quality classification is
`rows_in = rows_out + rows_quarantined`. The current run reconciles as
2,307,963 = 2,307,813 + 150. Conversion, query and database-load stages also record
their own input and output counts.

The committed `sample_data/` fixture is an older snapshot; its 100,000 rows include
one missing crash timestamp, which the producer counts and skips. The current full
source had no unparseable timestamps, and the full-stream run dropped zero rows. The
historical Part 2 figures for the 2026-11-08 download (2,303,506 rows) are not counts
for the current 2026-11-24 source. `run_all.sh` uses the current validated dataset even
in its 300-row quick mode so that its source checksum is meaningful.

## Street-name comparison

Q1 remains unchanged pending agreement to alter the committed answer. The report
compares raw street names with a report-only normalization of `PKWY`, `EXPY`, `EXWY`,
and `AVE`. The top-ten ranking changed: raw `GRAND CENTRAL PKWY` was rank 8 with
12,196 crashes, while standardized `GRAND CENTRAL PARKWAY` was rank 4 with 15,353.
This is evidence that spelling variation affects Q1, not a production change to its
grouping logic.

## Stream quality and cross-part checks

`scripts/run_stream_quality.sh` injects malformed JSON, an invalid timestamp, a
non-text borough, a late row, and a duplicate collision ID. The checker expects four
bad messages, one late message, and one duplicate; it also confirms the resulting Q3
matches a recount. The normal full-stream run independently passed the stream checker
9/9 and the database checker 10/10.

`serve/check_pipeline.py` checks that Part 1 and both stream stages used the same
download checksum, reconciles producer and consumer counts, checks Q2/Q3 totals, reuses
the stream/database checker results, and confirms the database load happened after
processing. The latest full-stream run passed all 8 checks.

The consumer writes periodic offset, lag, and throughput samples to
`results/stream_lag.csv`. Its metrics JSON also records bad, late, duplicate, and
unknown-borough message counts.

## History, dashboard, and alerts

Postgres stores per-run stage metrics in `pipeline_runs` and per-rule results in
`quality_results`; runs are retained rather than replacing the previous run. The
dashboard combines the latest report with history and shows quality pass/alert/fail
counts, stage rows and durations, stream lag history, memory use, database freshness,
and loaded tables. If Postgres is unavailable, the dashboard reports that state rather
than hiding it.

Monitoring thresholds are report-only alerts: consumer lag warns above 0 and fails
above 1,000 messages; database age warns after 1 day and fails after 7 days; container
memory warns at 80% of its limit; bad stream messages warn above 0 and fail above 1%;
and source row-count changes warn above 2% and fail above 5%. An alert is shown in the
report/dashboard and relevant checker output. A failed stage terminates the runner;
an OOM stop is therefore visible as a failed stage, although this laptop setup does not
send a separate OOM notification. A production deployment should retain explicit
failure events and forward alerts to email or an on-call service.

## Tests and limitations

The focused Spark test uses deliberately constructed rows to exercise rule counts,
accepted/quarantined separation, and threshold reporting. It is not a substitute for a
normal run on the downloaded source. The full-data run above exercised the ordinary
pipeline over all rows accepted by the quality rules.

The dashboard is a local demo service without authentication. Metrics JSON files in
`results/` describe the latest run; historical metrics are held in Postgres.
