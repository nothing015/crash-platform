# NYC Crash Data Platform

CPS 853 Team 2 project: a small data platform on the NYC Motor Vehicle Collisions dataset (about 2.27 million rows) from NYC Open Data. It answers three questions: which ten streets have the most crashes, at what hour of the day the most people are injured, and how many crashes happened in each borough in the latest hour.

**Status:** Parts 1–4 are implemented. The full-data pipeline and monitoring checks have been run successfully.

## Before you start

You need:

- Docker Desktop, running, with at least 4 GB of memory given to Docker (5 GB if your laptop can spare it)
- Git. On Windows, run every command in this README in **Git Bash**
- About 3 GB of free disk space (data, results and the Docker images) and an internet connection for the first run

Then get the code and go into its folder, in Git Bash (Windows) or Terminal (Mac):

```
git clone https://github.com/nothing015/crash-platform.git
cd crash-platform
```

## Quick start

From the `crash-platform` folder, run the complete pipeline:

```
./scripts/run_all.sh
```

`run_all.sh` runs Part 1, the stream fault-injection checks, Part 2, Part 3 and the cross-part checker. By default Part 2 replays 300 rows from the current validated dataset. To stream the complete accepted dataset instead (several minutes), run `FULL_STREAM=1 ./scripts/run_all.sh`. The first run downloads the CSV (about 540 MB, skipped if already present) and builds the Docker images.

Part 1 validates the raw rows and quarantines rejected records. Quality threshold breaches are reported in the quality report and dashboard but do not, by themselves, stop Part 1. To view monitoring after a run, start `./scripts/run_dashboard.sh` and open <http://localhost:8050>.

### Run the three parts manually

From the `crash-platform` folder, run these commands one at a time:

```
./scripts/start.sh
./scripts/run_stream_demo.sh
./scripts/run_serve.sh
```

1. **`start.sh` runs Part 1 (store and batch).** It downloads the data (about 540 MB, skipped if it is already present), validates the raw CSV, quarantines rejected rows, converts the accepted rows to Parquet and ORC in `data/lake/`, writes a 100,000-row sample for Part 2, and answers questions 1 and 2 from the Parquet files. It is done when the last line says `10 of 10 checks passed`. The first run also builds the Spark image, so it takes longer.
2. **`run_stream_demo.sh` runs Part 2 (stream).** By default, it starts Kafka and a consumer, then a producer replays the first 300 rows from the committed `sample_data/` fixture in event-time order (one event hour per second). The consumer answers question 3 and the stream checker reports `9 of 9 checks passed`. It takes under a minute. Do not reuse an old topic for a replay; the script creates a new topic for each run.
3. **`run_serve.sh` runs Part 3 (serve).** It starts Postgres, loads the result files from Parts 1 and 2, answers all three questions from the database, and checks every table against its file. It is done when the last line says `10 of 10 checks passed`.

The complete `./scripts/run_all.sh` runner differs from the manual Part 2 command in one important way: in its default 300-row mode, it streams the first 300 rows from the **current validated dataset** rather than the older committed `sample_data/` fixture. It also runs the stream fault-injection check and the cross-part checker. Use `FULL_STREAM=1 ./scripts/run_all.sh` to stream every accepted row.

## More options

**Part 1 experiments** (several minutes): compares Parquet with ORC, then three partitioning layouts and one Spark shuffle setting. Stop Kafka first if it is running (`docker compose stop kafka`) so the timings are fair.

```
docker compose run --rm spark bash scripts/run_benchmarks.sh
```

It is done when the last line says `4 of 4 checks passed`. To run the layout experiments on ORC instead of Parquet: `docker compose run --rm -e FORMAT=orc spark bash scripts/run_benchmarks.sh`

**Part 2 on every row** (needs `./scripts/start.sh` first, about 40 seconds of streaming):

```
SOURCE=full LIMIT=0 SPEEDUP=0 PRINT_EVERY=0 ./scripts/run_stream_demo.sh
```

To kill the consumer in the middle of a replay and check that its restart loses and repeats nothing: `./scripts/run_restart_demo.sh`. Unit tests: `docker compose run --rm stream python -m unittest discover -s stream`. Design, results and limitations are in `docs/part2_stream.md`.

**Part 3 database:** to look at the tables yourself, run `docker compose exec db psql -U crash crashes` (type `\dt` to list them, `\q` to quit). From a tool on your laptop, such as a VS Code PostgreSQL extension, connect to host `localhost`, port `5433`, database `crashes`, user `crash`, password `crash`. Run `./scripts/run_serve.sh` again after any new Part 1 or Part 2 run, since the database only changes when it loads. To delete the database and start fresh: `docker compose down -v`. Design, results and limitations are in `docs/part3_serve.md`.

**Part 4 quality and monitoring:** `jobs/quality.py` runs nine row-level checks before conversion, with additional volume and missingness-drift checks. Rejected rows are retained in `data/quarantine/quality_failures/`; accepted rows feed the answers from `data/lake/quality_validated/`. The report lists failed-row counts and thresholds. Threshold breaches are reported but do not stop Part 1 on their own. The dashboard shows check results, stage counts/durations, stream lag, memory, database freshness and run history. See `docs/part4_quality_monitoring.md`.

## Getting the latest version

- **First time on a computer:** clone the repo (see above).
- **Every time you start working:** run `cd crash-platform`, then `git pull`. To rerun Part 1, run `./scripts/start.sh` again; it skips the download.
- **Check that you are up to date:** run `git fetch`, then `git status`. It should say `Your branch is up to date with 'origin/main'`. If it says `behind`, run `git pull`.
- **Save your work:** run `git add <files>`, `git commit -m "what you changed"`, then `git push`. Changes only reach other computers once they are pushed, so push before you leave a computer, and `git pull` before you start on another.
- **Data is not in git:** it is downloaded by `start.sh` on each computer. The results and `sample_data/` come with the clone.

## What you get

| File                              | Contents                                                       |
| --------------------------------- | -------------------------------------------------------------- |
| `results/q1_top_streets.csv`      | The ten streets with the most crashes                          |
| `results/q2_injuries_by_hour.csv` | People injured per hour of day                                 |
| `results/metrics_part1.json`      | Raw input and accepted row counts, plus null counts            |
| `results/quality_report.json`     | Per-rule failed counts, thresholds, run status, and row reconciliation |
| `results/stage_metrics.json`      | Rows in and out at each pipeline stage                         |
| `results/pipeline_check_report.json` | Cross-part checksum, count, freshness and answer checks     |
| `results/stream_lag.csv`          | Stream offset, lag and throughput samples over time            |
| `results/container_memory.csv`   | Container memory snapshots by pipeline stage                   |
| `results/format_sizes.json`       | Sizes of the CSV, Parquet and ORC outputs                      |
| `results/format_comparison.csv`   | Parquet vs ORC: size, conversion time, query times             |
| `results/layout_comparison.csv`   | Three partitioning layouts: size, files, write and query times |
| `results/shuffle_comparison.csv`  | Question 1 with 200 and 8 shuffle partitions                   |
| `results/layout_plans.txt`        | Query plans showing whether Spark skipped partitions           |
| `results/q3_latest_hour.csv`      | Question 3: crashes per borough in the latest hour             |
| `results/q3_crashes_per_borough_hour.csv` | Crashes for every hour and borough                     |
| `results/stream_*_metrics.json`   | Part 2 rows, throughput, consumer lag, peak memory             |
| `data/quarantine/quality_failures/` | Rejected source rows with the rule IDs that failed          |
| `data/lake/quality_validated/`    | Accepted source rows used by the downstream pipeline          |
| `data/lake/`                      | Parquet, ORC and the Part 2 sample (not in git)                |
| Postgres `crashes` database       | Answer tables, `load_log`, and per-run `pipeline_runs` and `quality_results` history |

## Troubleshooting

- **`Permission denied` or `bad interpreter` on a script:** the script has Windows line endings. Run `git add --renormalize .` and check out the files again, or ask Nuaiman.
- **`docker: command not found` or the Docker daemon is not running:** start Docker Desktop and wait until it says it is running.
- **Spark stops with an out-of-memory error:** raise Docker's memory limit (Docker Desktop, Settings, Resources; on Windows with WSL 2, the `memory=` line in `%UserProfile%\.wslconfig`, then `wsl --shutdown`).
- **Git Bash shows paths like `C:/Program Files/Git/...`:** Git Bash rewrites arguments that start with `/`. Put `MSYS_NO_PATHCONV=1` in front of the command.
- **Need Spark's full logs while debugging?** By default only errors are shown. Run the job directly with the log level raised, for example `docker compose run --rm -e SPARK_LOG_LEVEL=info spark bash scripts/run_part1.sh`.
- **`password authentication failed for user "crash"` from a database tool on your laptop:** check that it connects to port **5433**, not 5432. The Docker database is on 5433 so it does not clash with a PostgreSQL installed on the laptop itself, which normally uses 5432.
- **`port is already allocated` for 5433 when starting the database:** something else on your laptop uses that port. Change `'5433:5432'` in `docker-compose.yml` to another port, such as `'5434:5432'`.
- **`data/raw/crashes.csv not found`:** run `./scripts/start.sh`, which downloads the data first.

## Project layout

```
docker-compose.yml     Spark, plus Kafka (Part 2) and Postgres (Part 3)
scripts/               start.sh, run_all.sh, run_dashboard.sh, run_part1.sh,
                       run_stream_demo.sh, run_stream_quality.sh,
                       run_restart_demo.sh, run_serve.sh
jobs/                  common.py, convert.py, queries.py, bench_formats.py,
                       bench_layouts.py, check_part1.py, quality.py
stream/                producer.py, consumer.py, state.py, inject_bad.py,
                       check_stream.py, check_stream_quality.py, tests
serve/                 schema.sql, load.py, query.py, check_serve.py,
                       check_pipeline.py, monitor.py
docs/                  part2_stream.md, part3_serve.md, part4_quality_monitoring.md
sample_data/           100,000-row Parquet sample for Part 2
results/               small CSV, JSON and text outputs (committed)
data/                  raw and converted data (not in git)
```

## Team

Ajmain Hyder, Nuaiman Hasan, Ali Asadpoor, Rishabh Seth
