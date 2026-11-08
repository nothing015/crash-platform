"""Plain-Python consumer: keeps a running count of crashes per borough per hour (question 3).

It periodically saves its counts and its read position together in one checkpoint file. After
a crash or a restart it carries on from that checkpoint, so no row is lost or counted twice.
To rebuild the counts from the first message, delete the file in data/stream_checkpoints/.

Examples (inside Docker, from the repo folder):
  docker compose run --rm stream python stream/consumer.py
  docker compose run --rm stream python stream/consumer.py --idle-exit 15
  docker compose run --rm stream python stream/consumer.py --print-every 0
"""
import argparse
import os
import signal
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from common import (CHECKPOINT_DIR, DEFAULT_TOPIC, RESULTS_DIR, borough_of, client_config,
                    ensure_topic, peak_memory_mb, rows_per_second, topic_identity, wait_for_kafka,
                    write_csv, write_json)
from state import StreamState, latest_hour_rows, load_checkpoint, save_checkpoint

CSV_HEADER = ['hour', 'borough', 'crashes']


def hour_line(hour, counter):
    """'2012-07-01T14' -> '2012-07-01 14:00  BROOKLYN 3, QUEENS 2'"""
    boroughs = ', '.join(f'{b} {n}' for b, n in sorted(counter.items()))
    return f'{hour[:10]} {hour[11:13]}:00  {boroughs}'


def current_lag(consumer, topic, state):
    """Messages in the topic not read yet, or None if the broker did not answer."""
    if not state.offsets:
        return None
    lag = 0
    try:
        for partition, next_offset in state.offsets.items():
            marks = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=2, cached=False)
            if marks is None:
                return None
            lag += max(marks[1] - next_offset, 0)
    except KafkaException:
        return None
    return lag


def write_live_results(state, run, lag, args):
    """The small files: cheap enough to refresh every couple of seconds."""
    write_csv(os.path.join(RESULTS_DIR, 'q3_latest_hour.csv'), CSV_HEADER, latest_hour_rows(state))
    seconds = (run['last'] - run['first']) if run['first'] is not None else 0
    write_json(os.path.join(RESULTS_DIR, 'stream_consumer_metrics.json'), {
        'topic': args.topic,
        'messages_consumed': state.stats['messages'],
        'bad_messages': state.stats['bad'],
        'unknown_borough': state.stats['unknown_borough'],
        'hours_seen': len(state.counts),
        'first_event_ts': state.stats['first_ts'],
        'last_event_ts': state.stats['last_ts'],
        'latest_hour': state.current_hour and state.current_hour + ':00:00',
        'resumed_from_offset': run['resumed_from'],
        'next_offset': sum(state.offsets.values()),
        'lag': lag,
        'messages_this_run': run['messages'],
        'seconds_this_run': round(seconds, 1),
        'messages_per_second': rows_per_second(run['messages'], seconds),
        'peak_memory_mb': peak_memory_mb(),
    })


def write_full_results(state, checkpoint, topic_id):
    """The large files: the checkpoint and the CSV with every hour."""
    save_checkpoint(checkpoint, state, topic_id)
    rows = ((hour + ':00:00', borough, crashes)
            for hour in sorted(state.counts)
            for borough, crashes in sorted(state.counts[hour].items()))
    write_csv(os.path.join(RESULTS_DIR, 'q3_crashes_per_borough_hour.csv'), CSV_HEADER, rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--topic', default=DEFAULT_TOPIC)
    parser.add_argument('--group', default='crash-consumer')
    parser.add_argument('--print-every', type=int, default=1,
                        help='print every Nth message (default 1 = every message, 0 = none)')
    parser.add_argument('--idle-exit', type=float, default=0,
                        help='stop after this many seconds without new messages (default: run until Ctrl+C)')
    parser.add_argument('--checkpoint-every', type=float, default=30,
                        help='seconds between checkpoints and full CSV writes (default 30)')
    args = parser.parse_args()

    admin = wait_for_kafka()
    ensure_topic(admin, args.topic)   # so the consumer can start before the producer
    topic_id = topic_identity(admin, args.topic)
    checkpoint = os.path.join(CHECKPOINT_DIR, f'{args.group}__{args.topic}.json')
    state = load_checkpoint(checkpoint, topic_id) or StreamState()

    def start_from_checkpoint(consumer, partitions):
        for partition in partitions:
            partition.offset = state.offsets.get(partition.partition, OFFSET_BEGINNING)
        consumer.assign(partitions)

    consumer = Consumer(client_config(**{
        'group.id': args.group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,   # the position lives in the checkpoint, next to the counts
    }))
    consumer.subscribe([args.topic], on_assign=start_from_checkpoint)

    stop_requested = []                # set by Ctrl+C or docker stop, checked between messages
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_: stop_requested.append(True))

    run = {'first': None, 'last': None, 'messages': 0, 'resumed_from': sum(state.offsets.values())}
    started = last_activity = last_live = last_full = time.monotonic()
    if run['resumed_from']:
        print(f"Resuming from offset {run['resumed_from']:,}: "
              f"{state.stats['messages']:,} messages already counted.", flush=True)
    print(f'Listening on topic {args.topic!r}. Press Ctrl+C to stop.', flush=True)

    try:
        while not stop_requested:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if args.idle_exit:
                    # before the first message, allow extra time for the producer to start
                    limit = args.idle_exit + (0 if run['messages'] else 120)
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
            run['first'] = run['first'] if run['first'] is not None else now
            run['last'] = now
            run['messages'] += 1

            previous_hour = state.current_hour
            data = state.apply(msg.partition(), msg.offset(), msg.value())
            if data is not None:
                if previous_hour is not None and state.current_hour != previous_hour:
                    print('>> ' + hour_line(previous_hour, state.counts[previous_hour]), flush=True)
                if args.print_every and state.stats['messages'] % args.print_every == 0:
                    street = data.get('on_street_name') or '-'
                    print(f"{data['crash_ts']}  {borough_of(data.get('borough')):<14} {street}", flush=True)

            if now - last_live >= 2:
                write_live_results(state, run, current_lag(consumer, args.topic, state), args)
                last_live = now
            if now - last_full >= args.checkpoint_every:
                write_full_results(state, checkpoint, topic_id)
                last_full = now
    finally:
        lag = current_lag(consumer, args.topic, state)
        consumer.close()
        write_full_results(state, checkpoint, topic_id)
        write_live_results(state, run, lag, args)

    if stop_requested:
        print('\nStopped by user.')
    print('=' * 60)
    if state.current_hour is not None:
        print('Latest hour: ' + hour_line(state.current_hour, state.counts[state.current_hour]))
    print(f"Consumed {state.stats['messages']:,} messages ({state.stats['bad']:,} unreadable, "
          f"{state.stats['unknown_borough']:,} with no borough) over {len(state.counts):,} hours "
          f'in {time.monotonic() - started:,.1f} s')
    print('Results: results/q3_latest_hour.csv, results/q3_crashes_per_borough_hour.csv, '
          'results/stream_consumer_metrics.json')


if __name__ == '__main__':
    main()
