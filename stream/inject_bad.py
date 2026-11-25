"""Send malformed, late, and duplicate records for the stream quality check."""
import argparse
import json

from confluent_kafka import Producer

from common import client_config, ensure_topic, wait_for_kafka


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topic', required=True)
    args = parser.parse_args()

    admin = wait_for_kafka()
    ensure_topic(admin, args.topic)
    producer = Producer(client_config())
    messages = [
        b'not-json',
        json.dumps({
            'collision_id': 'qa-bad-time',
            'crash_ts': 'not-a-timestamp',
            'borough': 'BRONX',
        }).encode(),
        json.dumps({
            'collision_id': 'qa-bad-borough',
            'crash_ts': '2012-07-01T01:10:00',
            'borough': 5,
        }).encode(),
        json.dumps({
            'collision_id': 'qa-valid-1',
            'crash_ts': '2012-07-01T01:10:00',
            'borough': 'BRONX',
        }).encode(),
        json.dumps({
            'collision_id': 'qa-late',
            'crash_ts': '2012-07-01T00:10:00',
            'borough': 'QUEENS',
        }).encode(),
        json.dumps({
            'collision_id': 'qa-valid-1',
            'crash_ts': '2012-07-01T01:15:00',
            'borough': 'BRONX',
        }).encode(),
    ]
    for value in messages:
        producer.produce(args.topic, value=value)
    remaining = producer.flush(30)
    if remaining:
        raise SystemExit(f'FAIL: {remaining} injected messages were not delivered')
    print(f'Sent {len(messages)} stream QA messages to {args.topic}')


if __name__ == '__main__':
    main()
