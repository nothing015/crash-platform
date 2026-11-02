"""Plain-Python consumer: keeps a running count of crashes per borough per hour (question 3).

It never commits offsets, so every run reads the topic from the beginning and the counts
are always complete. (Restarting without losing rows is an optional extra, see the runbook.)

Examples (inside Docker, from the repo folder):
  docker compose run --rm stream python stream/consumer.py
  docker compose run --rm stream python stream/consumer.py --idle-exit 15
"""
import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException

from common import (DEFAULT_TOPIC, RESULTS_DIR, borough_of, client_config, ensure_topic,
                    hour_key, wait_for_kafka, write_json)


def hour_line(hour, counter):
    """'2012-07-01T14' -> '2012-07-01 14:00  BROOKLYN 3, QUEENS 2'"""
    boroughs = ', '.join(f'{b} {n}' for b, n in sorted(counter.items()))
    return f'{hour[:10]} {hour[11:13]}:00  {boroughs}'


def write_results(counts, stats, args):
    path = os.path.join(RESULTS_DIR, 'q3_crashes_per_borough_hour.csv')
    os.makedirs(RESULTS_DIR, exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['hour', 'borough', 'crashes'])
        for hour in sorted(counts):
            for borough, crashes in sorted(counts[hour].items()):
                writer.writerow([hour + ':00:00', borough, crashes])
    os.replace(tmp, path)
    write_json(os.path.join(RESULTS_DIR, 'stream_consumer_metrics.json'), {
        'topic': args.topic,
        'messages_consumed': stats['messages'],
        'bad_messages': stats['bad'],
        'unknown_borough': stats['unknown_borough'],
        'hours_seen': len(counts),
        'first_event_ts': stats['first_ts'],
        'last_event_ts': stats['last_ts'],
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--topic', default=DEFAULT_TOPIC)
    parser.add_argument('--group', default='crash-consumer')
    parser.add_argument('--print-every', type=int, default=1,
                        help='print every Nth message (default 1 = every message, 0 = none)')
    parser.add_argument('--idle-exit', type=float, default=0,
                        help='stop after this many seconds without new messages (default: run until Ctrl+C)')
    args = parser.parse_args()

    admin = wait_for_kafka()
    ensure_topic(admin, args.topic)   # so the consumer can start before the producer
    consumer = Consumer(client_config(**{
        'group.id': args.group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,   # never commit: every run starts from the first message
    }))
    consumer.subscribe([args.topic])

    counts = defaultdict(Counter)      # counts[hour][borough] = crashes
    stats = {'messages': 0, 'bad': 0, 'unknown_borough': 0, 'first_ts': None, 'last_ts': None}
    current_hour = None
    started = last_activity = last_write = time.monotonic()
    print(f'Listening on topic {args.topic!r}. Press Ctrl+C to stop.', flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if args.idle_exit:
                    # before the first message, allow extra time for the producer to start
                    limit = args.idle_exit + (0 if stats['messages'] else 120)
                    if now - last_activity > limit:
                        print('No new messages, stopping.', flush=True)
                        break
                continue
            if msg.error():
                code = msg.error().code()
                if code in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise KafkaException(msg.error())
            last_activity = now

            try:
                data = json.loads(msg.value())
                hour = hour_key(data['crash_ts'])
            except (ValueError, KeyError, TypeError):
                stats['bad'] += 1
                continue

            borough = borough_of(data.get('borough'))
            if borough == 'UNKNOWN':
                stats['unknown_borough'] += 1
            if current_hour is not None and hour > current_hour:
                print('>> ' + hour_line(current_hour, counts[current_hour]), flush=True)
            if current_hour is None or hour > current_hour:
                current_hour = hour
            counts[hour][borough] += 1
            stats['messages'] += 1
            stats['first_ts'] = stats['first_ts'] or data['crash_ts']
            stats['last_ts'] = data['crash_ts']

            if args.print_every and stats['messages'] % args.print_every == 0:
                street = data.get('on_street_name') or '-'
                print(f"{data['crash_ts']}  {borough:<14} {street}", flush=True)
            if now - last_write >= 2:
                write_results(counts, stats, args)
                last_write = now
    except KeyboardInterrupt:
        print('\nStopped by user.')
    finally:
        consumer.close()
        write_results(counts, stats, args)

    print('=' * 60)
    if current_hour is not None:
        print('Latest hour: ' + hour_line(current_hour, counts[current_hour]))
    print(f"Consumed {stats['messages']:,} messages ({stats['bad']:,} unreadable, "
          f"{stats['unknown_borough']:,} with no borough) over {len(counts):,} hours "
          f'in {time.monotonic() - started:,.1f} s')
    print('Results: results/q3_crashes_per_borough_hour.csv, results/stream_consumer_metrics.json')


if __name__ == '__main__':
    main()