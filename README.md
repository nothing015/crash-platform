# NYC Crash Data Platform

CPS 853 Team 2 project: a small data platform on the NYC Motor Vehicle Collisions dataset (about 2.27 million rows) from NYC Open Data. It answers three questions: which ten streets have the most crashes, at what hour of the day the most people are injured, and how many crashes happened in each borough in the latest hour.

**Status:** Part 1 (store and batch) is ready. Part 2 (stream), Part 3 (serve) and Part 4 (quality and monitoring) are in progress.

## Requirements

- Docker Desktop, with at least 4 GB of memory given to Docker (5 GB if your laptop can spare it)
- Git. On Windows, run every command below in **Git Bash**
- About 3 GB of free disk space (data, results and the Spark image) and an internet connection for the first run

## Quick start (Part 1)

Open Git Bash (Windows) or Terminal (Mac) in a folder you like, and run these three commands one at a time:

```
git clone https://github.com/nothing015/crash-platform.git
cd crash-platform
./scripts/start.sh
```

`start.sh` downloads the data (about 540 MB, skipped if it is already there), then runs Part 1 in Docker: it converts the CSV to Parquet and ORC in `data/lake/`, writes a 100,000-row sample for Part 2, answers questions 1 and 2 from the Parquet files, and checks the outputs. It is done when the last line says `8 of 8 checks passed`. The first run also downloads the Spark image, so it takes longer.

**Optional experiments** (several minutes): compares Parquet with ORC, then three partitioning layouts and one Spark shuffle setting. Stop Kafka first if it is running (`docker compose stop kafka`) so the timings are fair.

```
docker compose run --rm spark bash scripts/run_benchmarks.sh
```

It is done when the last line says `4 of 4 checks passed`. To run the layout experiments on ORC instead of Parquet: `docker compose run --rm -e FORMAT=orc spark bash scripts/run_benchmarks.sh`

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
| `results/metrics_part1.json`      | Row count and null counts after reading the CSV                |
| `results/format_sizes.json`       | Sizes of the CSV, Parquet and ORC outputs                      |
| `results/format_comparison.csv`   | Parquet vs ORC: size, conversion time, query times             |
| `results/layout_comparison.csv`   | Three partitioning layouts: size, files, write and query times |
| `results/shuffle_comparison.csv`  | Question 1 with 200 and 8 shuffle partitions                   |
| `results/layout_plans.txt`        | Query plans showing whether Spark skipped partitions           |
| `data/lake/`                      | Parquet, ORC and the Part 2 sample (not in git)                |

## Troubleshooting

- **`Permission denied` or `bad interpreter` on a script:** the script has Windows line endings. Run `git add --renormalize .` and check out the files again, or ask Nuaiman.
- **`docker: command not found` or the Docker daemon is not running:** start Docker Desktop and wait until it says it is running.
- **Spark stops with an out-of-memory error:** raise Docker's memory limit (Docker Desktop, Settings, Resources; on Windows with WSL 2, the `memory=` line in `%UserProfile%\.wslconfig`, then `wsl --shutdown`).
- **Git Bash shows paths like `C:/Program Files/Git/...`:** Git Bash rewrites arguments that start with `/`. Put `MSYS_NO_PATHCONV=1` in front of the command.
- **Need Spark's full logs while debugging?** By default only errors are shown. Run the job directly with the log level raised, for example `docker compose run --rm -e SPARK_LOG_LEVEL=info spark bash scripts/run_part1.sh`.
- **`data/raw/crashes.csv not found`:** run `./scripts/start.sh`, which downloads the data first.

## Project layout

```
docker-compose.yml     Spark (and a Kafka container for Part 2)
scripts/               start.sh, download_data.sh, run_part1.sh, run_benchmarks.sh
jobs/                  common.py, convert.py, queries.py, bench_formats.py,
                       bench_layouts.py, check_part1.py
sample_data/           100,000-row Parquet sample for Part 2
results/               small CSV, JSON and text outputs (committed)
data/                  raw and converted data (not in git)
```

## Team

Ajmain Hyder, Nuaiman Hasan, Ali Asadpoor, Rishabh Seth
