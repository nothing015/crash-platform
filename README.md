# NYC Crash Data Platform

CPS 853 Team 2 project: a small data platform on the NYC Motor Vehicle Collisions dataset (about 2.27 million rows) from NYC Open Data. It answers three questions: which ten streets have the most crashes, at what hour of the day the most people are injured, and how many crashes happened in each borough in the latest hour.

**Status:** Part 1 (store and batch) is ready. Part 2 (stream), Part 3 (serve) and Part 4 (quality and monitoring) are in progress.

## Requirements

- Docker Desktop, with at least 4 GB of memory given to Docker (5 GB if your laptop can spare it)
- Git. On Windows, run every command below in **Git Bash**
- About 3 GB of free disk space (data, results and the Spark image) and an internet connection for the first run

## Quick start (Part 1)

Run these three commands from the repo folder, in order:

```
./scripts/download_data.sh
docker compose run --rm spark bash scripts/run_part1.sh
docker compose run --rm spark bash scripts/run_benchmarks.sh
```

1. **Download:** fetches the raw CSV (about 540 MB) into `data/raw/` and records its size and checksum in `results/data_info.txt`. The first `docker compose run` also downloads the Spark image.
2. **Convert and query:** a Spark job (local mode, in Docker) converts the CSV to Parquet and ORC in `data/lake/`, writes a 100,000-row sample for Part 2, and answers questions 1 and 2 from the Parquet files. It ends with a PASS/FAIL check of the outputs.
3. **Experiments:** compares Parquet with ORC, then three partitioning layouts and one Spark shuffle setting. It takes several minutes. Stop Kafka first if it is running (`docker compose stop kafka`) so the timings are fair.

To run the layout experiments on ORC instead of Parquet: `docker compose run --rm -e FORMAT=orc spark bash scripts/run_benchmarks.sh`

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
- **`data/raw/crashes.csv not found`:** run `./scripts/download_data.sh` first.

## Project layout

```
docker-compose.yml     Spark (and a Kafka container for Part 2)
scripts/               download_data.sh, run_part1.sh, run_benchmarks.sh
jobs/                  common.py, convert.py, queries.py, bench_formats.py,
                       bench_layouts.py, check_part1.py
sample_data/           100,000-row Parquet sample for Part 2
results/               small CSV, JSON and text outputs (committed)
data/                  raw and converted data (not in git)
```

## Team

Ajmain Hyder, Nuaiman Hasan, Ali Asadpoor, Rishabh Seth
