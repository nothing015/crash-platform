# Part 2: Stream (Kafka)

Owner: Ali Asadpoor. Starter code by Nuaiman Hasan. Draft section for the final report.

## What it does

A producer reads the crash rows from the Parquet files written by Part 1, sorts them by event time (`crash_ts`, built from CRASH DATE and CRASH TIME) and replays them into a Kafka topic. A plain-Python consumer reads the topic and keeps a running count of crashes per borough per hour, which answers question 3: how many crashes happened in each borough in the latest hour?

## Results on the full dataset

| Measure | Result |
| --- | --- |
| Rows replayed | 2,303,506 of 2,303,506 |
| Event time covered | 2012-07-01 to 2026-11-03 (5,239 days) |
| Producer | 13.2 s, about 175,000 rows per second |
| Consumer | 21.7 s, about 106,000 messages per second |
| Consumer lag at the end | 0 messages |
| Groups compared with a batch recount | 572,320 hour and borough groups, all equal |
| Automated checks | 9 of 9 passed |
| Peak memory | Producer 1.12 GB, consumer 161 MB; highest sampled: stream container 1.43 GB (limit 2 GB), Kafka 0.4 GB (limit 1 GB, heap 512 MB) |

These timings are from running as fast as possible (`SPEEDUP=0`), with Kafka, the producer and the consumer in containers on one machine. Replaying 14 years at one event hour per second would take about 35 hours, so a live demo replays a slice instead.

Latest hour in this run (2026-11-03 23:00): Queens 3, unknown borough 1, and 0 in the other four boroughs.

## Design decisions

**One broker, one partition.** The proposal's first risk is fitting the stack on one 8 GB laptop, so we run a single broker. A single partition keeps every row in event-time order, because Kafka only guarantees order inside a partition. The cost is that only one consumer in a group can do the work. The consumer already handles 2.3 million rows in about 22 seconds, so we do not need more.

**Writing: `acks=all` with an idempotent producer.** A row counts as sent only once the broker has stored it, and a retry cannot write the row twice. `confluent-kafka` leaves idempotence off by default, so we turn it on.

**Reading: counts and position are saved together.** Every 30 seconds the consumer writes its counts and its next offset in one atomic file replace. A restart loads that file and carries on from that offset. Whatever was processed after the last save is reprocessed from a state that does not contain it, so no row is lost or counted twice. Committing offsets to Kafka alone would not be enough here: the counts live in memory, so a restart would resume from the committed offset with empty counts. A checkpoint is ignored if the topic was deleted and recreated, which we detect from Kafka's topic ID.

**Latest hour.** The latest hour is the most recent calendar hour seen in event time, not in wall-clock time. It is written to `results/q3_latest_hour.csv` with every borough listed, 0 where no crash happened. While a replay runs, or if it stops early, that hour can still be partly filled. A late row is added to its own hour and never moves the latest hour backwards.

**Bad input.** A message that is not JSON, or has a malformed timestamp or a non-text borough, is counted as bad and skipped. It cannot affect the counts or the latest hour.

## Fault test

`scripts/run_restart_demo.sh` kills the consumer with SIGKILL in the middle of a 10,000-row replay and starts it again. In our run it was killed after its checkpoint at offset 1,913, resumed there, and finished with all 10,000 rows. All 9 checks passed, including the comparison with the batch recount.

## Data notes and limitations

- **Missing boroughs.** 674,862 rows (29.3%) have no borough. They are counted as UNKNOWN instead of being dropped, so the latest-hour total stays correct but the per-borough numbers are lower bounds.
- **Time zone.** CRASH DATE and CRASH TIME are New York local time. Part 1 parses them with Spark's session time zone set to UTC, which keeps the clock reading unchanged, and the stream carries that value as `YYYY-MM-DDTHH:MM:SS`. Hours in question 3 are therefore New York local hours, matching question 2. The format is the same across the team, but the values are not true UTC instants.
- **Snapshot.** These results are a historical run on the 2026-11-08 download (2,303,506 rows). The proposal and committed Part 1 results at the time used the 2026-10-04 download (2,269,188 rows); see Part 4 for the latest full-data run.
- **Committed sample.** `sample_data/` is a separate, older 100,000-row fixture and contains one row without a crash timestamp, which the producer counts as dropped. The current full-data quality run had no unparseable timestamps. `scripts/run_all.sh` uses the current validated dataset for its quick 300-row stream instead of this committed fixture.
- **No disorder in the replay.** Rows are sorted before they are sent, so a late row does not occur in the normal replay. The Part 4 fault-injection check sends a late row and verifies it is counted.
- **Repeated replays.** Replaying into the same topic would add the rows again. The demo scripts avoid this with a new topic for every run.

## Reproducing it

```
./scripts/run_stream_demo.sh                                                  # 300 rows, under a minute
SOURCE=full LIMIT=0 SPEEDUP=0 PRINT_EVERY=0 ./scripts/run_stream_demo.sh      # every row (needs ./scripts/start.sh first)
./scripts/run_restart_demo.sh                                                 # the fault test
docker compose run --rm stream python -m unittest discover -s stream          # unit tests
```

## Output for Part 3

| File | Columns | Contents |
| --- | --- | --- |
| `results/q3_latest_hour.csv` | `hour, borough, crashes` | The answer to question 3: one row per borough for the latest hour |
| `results/q3_crashes_per_borough_hour.csv` | `hour, borough, crashes` | Every hour and borough that had a crash |
| `results/stream_consumer_metrics.json` | | Messages, lag, throughput, peak memory |
| `results/stream_producer_metrics.json` | | Rows sent and delivered, throughput, peak memory |

`hour` looks like `2026-11-03T23:00:00`. Boroughs are BRONX, BROOKLYN, MANHATTAN, QUEENS, STATEN ISLAND and UNKNOWN.

## AI use

Claude Code (Anthropic) was used to review the starter code, implement the restart, latest-hour, metrics and validation changes with their tests, run the experiments and draft this section.
