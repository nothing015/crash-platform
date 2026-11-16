# Part 3: Serve (Postgres)

Owner: Ajmain Hyder. Draft section for the final report.

## What it does

Postgres runs as the `db` service in Docker. A loader reads the four result files from Parts 1 and 2 and copies them into one table each. A query script then answers all three questions from the database, and a check script reads every table back and compares it with its file.

```
Part 1 (Spark)  -> results/q1_top_streets.csv, results/q2_injuries_by_hour.csv  \
                                                                                  > serve/load.py -> Postgres -> serve/query.py
Part 2 (Kafka)  -> results/q3_latest_hour.csv, q3_crashes_per_borough_hour.csv  /
```

## Tables

| Table | Loaded from | Columns | Key |
| --- | --- | --- | --- |
| `q1_top_streets` | `results/q1_top_streets.csv` | `street, crashes` | `street` |
| `q2_injuries_by_hour` | `results/q2_injuries_by_hour.csv` | `hour_of_day, injured` | `hour_of_day` (0 to 23) |
| `q3_crashes_per_borough_hour` | `results/q3_crashes_per_borough_hour.csv` | `hour, borough, crashes` | `hour, borough` |
| `q3_latest_hour` | `results/q3_latest_hour.csv` | `hour, borough, crashes` | `borough` |
| `load_log` | written by the loader | `loaded_at, table_name, source_file, rows_loaded, seconds` | |

`hour` is a `timestamp` without a time zone. As in Parts 1 and 2, it holds New York local time (see "Time zone" in `docs/part2_stream.md`). Counts have a `CHECK (crashes >= 0)` rule, and the keys stop a file with duplicate rows from loading.

## Results

| Measure | Demo results (300 streamed rows) | Full results (every row) |
| --- | --- | --- |
| Rows in `q3_crashes_per_borough_hour` | 81 | 572,320 |
| Load time for that table | under 0.01 s | 1.0 s |
| Postgres memory after loading | | 87 MB (limit 512 MB) |
| Automated checks | 10 of 10 passed | 10 of 10 passed |

The full results are the files committed in `115d1b6` (the 2026-11-08 download, 2,303,506 rows).

## Design decisions

**Postgres.** The proposal's first risk is fitting Spark, Kafka and the database on one 8 GB laptop. Postgres needs under 100 MB here, and the container is capped at 512 MB. It runs as a real server, so a dashboard or API in a later step can query it while Spark and Kafka run. SQLite is a single file with no server. DuckDB reads CSV and Parquet directly but is built for analysis, not for serving an app.

**Loading the CSV files instead of writing from the consumer.** Questions 1 and 2 come out of Spark as CSV files, so one loader handles all three questions the same way. Postgres's `COPY` command loads the largest file (572,320 rows) in about a second. The consumer stays as it is. Its restart guarantee depends on saving its counts and its Kafka position together in one file. Writing straight to the database would mean saving both in one database transaction and testing the restart again. The cost is freshness: the hourly file is rewritten every 30 seconds while a replay runs, so the database is only as current as the last load. If Part 3 needs live numbers during a replay, the consumer can be changed to update the counts and its position in the same transaction.

**All tables in one transaction.** The loader empties and refills all four tables inside one transaction. Readers see either the old answers or the new ones, never a half-loaded table. If anything fails, for example a negative count or a duplicate row, nothing changes. We tested this: after appending a row with a count of −5 to the full hourly file, the load stopped with a `CheckViolation` error, and all 572,320 rows of the previous load were still there.

**Header check before loading.** The loader stops if a file's columns are not exactly the ones agreed with Parts 1 and 2. A renamed column fails with a clear message instead of loading into the wrong table column.

## Checks

`serve/check_serve.py` runs 10 checks:

- Each of the four tables holds exactly the rows of its file, compared row by row (4 checks).
- Question 1 has 10 streets.
- Question 2 has all 24 hours.
- The latest hour in `q3_latest_hour` is the last hour in `q3_crashes_per_borough_hour`.
- The latest hour lists all six boroughs, including UNKNOWN.
- The latest-hour counts equal the hourly table's counts for that hour (0 where the hourly table has no row).
- The last load is recorded in `load_log` for all four tables.

## Limitations

- **Only as fresh as the last load.** Run `./scripts/run_serve.sh` again after a new Part 1 or Part 2 run.
- **One snapshot at a time.** Every load replaces the previous answers. `load_log` records when each load happened, but older answers are not kept.
- **The database holds answers, not crash rows.** Questions about other streets, years or columns need a new Spark job or a crash table in the database.
- **Demo credentials.** User and password are both `crash`, set in `docker-compose.yml`, and the database is open on port 5433 of the laptop (5433 so it does not clash with a PostgreSQL installed on the laptop itself). This is fine for a local demo, not for anything shared.

## Reproducing it

```
./scripts/run_serve.sh                                         # start Postgres, load, answer, check
docker compose exec db psql -U crash crashes                   # look at the tables yourself
docker compose run --rm serve python serve/query.py            # answer the three questions again
docker compose down -v                                         # stop everything and delete the database
```

## AI use

Claude Code (Anthropic) was used to choose the database, write the loader, query and check scripts, run the tests and draft this section.
