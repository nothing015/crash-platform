"""Tests for the Part 2 logic that does not need a running broker.

  docker compose run --rm stream python -m unittest discover -s stream -v

The kill-and-restart test against a real broker is scripts/run_restart_demo.sh.
"""
import json
import os
import tempfile
import unittest

from check_stream import latest_hour_matches
from common import hour_key, peak_memory_mb, rows_per_second
from producer import producer_config
from state import StreamState, latest_hour_rows, load_checkpoint, save_checkpoint


def message(ts, borough: object = 'BROOKLYN', collision_id=None):
    return json.dumps({
        'collision_id': collision_id or ts,
        'crash_ts': ts,
        'borough': borough,
        'on_street_name': 'MAIN ST',
    }).encode()


# Ten crashes over three hours; the offset of each one is its position in this list.
STREAM = [
    message('2012-07-01T00:05:00', 'BROOKLYN'),
    message('2012-07-01T00:10:00', 'QUEENS'),
    message('2012-07-01T00:50:00', 'BROOKLYN'),
    message('2012-07-01T01:01:00', None),
    message('2012-07-01T01:15:00', 'BRONX'),
    message('2012-07-01T01:20:00', 'BRONX'),
    message('2012-07-01T02:00:00', 'QUEENS'),
    message('2012-07-01T02:30:00', 'QUEENS'),
    message('2012-07-01T02:45:00', 'MANHATTAN'),
    message('2012-07-01T02:59:00', 'STATEN ISLAND'),
]


def feed(state, start=0, stop=None):
    for offset, value in list(enumerate(STREAM))[start:stop]:
        state.apply(0, offset, value)
    return state


class HourKeyTests(unittest.TestCase):
    def test_returns_the_hour_of_a_full_timestamp(self):
        self.assertEqual(hour_key('2012-07-01T14:23:00'), '2012-07-01T14')

    def test_rejects_text_that_is_not_a_timestamp(self):
        with self.assertRaises(ValueError):
            hour_key('garbage-text-here!!')

    def test_rejects_a_date_without_a_time(self):
        with self.assertRaises(ValueError):
            hour_key('2012-07-01')

    def test_rejects_a_value_that_is_not_text(self):
        with self.assertRaises(TypeError):
            hour_key(20120701)


class ApplyTests(unittest.TestCase):
    def test_counts_crashes_per_hour_and_borough(self):
        state = feed(StreamState())
        self.assertEqual(state.counts['2012-07-01T00']['BROOKLYN'], 2)
        self.assertEqual(state.counts['2012-07-01T02']['QUEENS'], 2)

    def test_missing_borough_is_counted_as_unknown(self):
        state = feed(StreamState())
        self.assertEqual(state.counts['2012-07-01T01']['UNKNOWN'], 1)
        self.assertEqual(state.stats['unknown_borough'], 1)

    def test_next_offset_moves_past_the_last_message(self):
        self.assertEqual(feed(StreamState()).offsets, {0: 10})

    def test_malformed_timestamp_is_bad_and_cannot_become_the_latest_hour(self):
        state = feed(StreamState())
        state.apply(0, 10, message('garbage-text-here!!'))
        self.assertEqual(state.stats['bad'], 1)
        self.assertEqual(state.current_hour, '2012-07-01T02')
        self.assertNotIn('garbage-text-', state.counts)

    def test_borough_that_is_not_text_is_bad(self):
        state = StreamState()
        state.apply(0, 0, message('2012-07-01T00:05:00', 5))
        self.assertEqual(state.stats['bad'], 1)
        self.assertEqual(state.stats['messages'], 0)

    def test_unreadable_message_still_moves_the_offset(self):
        state = StreamState()
        state.apply(0, 0, b'not json')
        self.assertEqual(state.stats['bad'], 1)
        self.assertEqual(state.offsets, {0: 1})

    def test_a_message_delivered_twice_is_counted_once(self):
        state = StreamState()
        state.apply(0, 0, STREAM[0])
        state.apply(0, 0, STREAM[0])
        self.assertEqual(state.stats['messages'], 1)
        self.assertEqual(state.counts['2012-07-01T00']['BROOKLYN'], 1)

    def test_a_late_message_is_counted_without_moving_the_latest_hour_back(self):
        state = feed(StreamState())
        state.apply(0, 10, message('2012-07-01T00:20:00', 'BRONX'))
        self.assertEqual(state.counts['2012-07-01T00']['BRONX'], 1)
        self.assertEqual(state.current_hour, '2012-07-01T02')
        self.assertEqual(state.stats['late_messages'], 1)

    def test_duplicate_collision_id_is_counted_as_bad_and_skipped(self):
        state = StreamState()
        state.apply(0, 0, message('2012-07-01T00:05:00', collision_id='same'))
        state.apply(0, 1, message('2012-07-01T00:10:00', collision_id='same'))
        self.assertEqual(state.stats['duplicate_collision_ids'], 1)
        self.assertEqual(state.stats['bad'], 1)
        self.assertEqual(state.stats['messages'], 1)


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = os.path.join(folder.name, 'checkpoints', 'group__topic.json')

    def test_crash_after_a_checkpoint_gives_the_same_counts_as_no_crash(self):
        state = feed(StreamState(), stop=5)
        save_checkpoint(self.path, state, 'topic-a')
        feed(state, start=5, stop=8)   # processed but never saved: lost in the crash

        restored = load_checkpoint(self.path, 'topic-a')
        feed(restored, start=restored.offsets[0])

        expected = feed(StreamState())
        self.assertEqual(restored.counts, expected.counts)
        self.assertEqual(restored.stats, expected.stats)
        self.assertEqual(restored.seen_collision_ids, expected.seen_collision_ids)
        self.assertEqual(restored.offsets, expected.offsets)
        self.assertEqual(restored.current_hour, expected.current_hour)

    def test_no_checkpoint_means_start_from_nothing(self):
        self.assertIsNone(load_checkpoint(self.path, 'topic-a'))

    def test_checkpoint_of_a_recreated_topic_is_ignored(self):
        save_checkpoint(self.path, feed(StreamState()), 'topic-a')
        self.assertIsNone(load_checkpoint(self.path, 'topic-b'))

    def test_saving_leaves_no_temporary_file(self):
        save_checkpoint(self.path, feed(StreamState()), 'topic-a')
        self.assertEqual(os.listdir(os.path.dirname(self.path)), ['group__topic.json'])


class LatestHourTests(unittest.TestCase):
    def test_lists_only_the_latest_hour_with_zero_for_quiet_boroughs(self):
        self.assertEqual(latest_hour_rows(feed(StreamState())), [
            ('2012-07-01T02:00:00', 'BRONX', 0),
            ('2012-07-01T02:00:00', 'BROOKLYN', 0),
            ('2012-07-01T02:00:00', 'MANHATTAN', 1),
            ('2012-07-01T02:00:00', 'QUEENS', 2),
            ('2012-07-01T02:00:00', 'STATEN ISLAND', 1),
            ('2012-07-01T02:00:00', 'UNKNOWN', 0),
        ])

    def test_keeps_a_borough_name_outside_the_usual_five(self):
        state = StreamState()
        state.apply(0, 0, message('2012-07-01T00:05:00', 'ATLANTIS'))
        self.assertIn(('2012-07-01T00:00:00', 'ATLANTIS', 1), latest_hour_rows(state))

    def test_is_empty_before_any_message(self):
        self.assertEqual(latest_hour_rows(StreamState()), [])


class LatestHourCheckTests(unittest.TestCase):
    ALL_ROWS = [
        {'hour': '2012-07-01T00:00:00', 'borough': 'QUEENS', 'crashes': '4'},
        {'hour': '2012-07-01T02:00:00', 'borough': 'BRONX', 'crashes': '3'},
    ]

    def test_accepts_a_file_that_matches_the_last_hour(self):
        latest = [
            {'hour': '2012-07-01T02:00:00', 'borough': 'BRONX', 'crashes': '3'},
            {'hour': '2012-07-01T02:00:00', 'borough': 'QUEENS', 'crashes': '0'},
        ]
        self.assertTrue(latest_hour_matches(self.ALL_ROWS, latest))

    def test_rejects_a_different_count(self):
        latest = [{'hour': '2012-07-01T02:00:00', 'borough': 'BRONX', 'crashes': '9'}]
        self.assertFalse(latest_hour_matches(self.ALL_ROWS, latest))

    def test_rejects_a_file_for_an_older_hour(self):
        latest = [{'hour': '2012-07-01T00:00:00', 'borough': 'QUEENS', 'crashes': '4'}]
        self.assertFalse(latest_hour_matches(self.ALL_ROWS, latest))


class ProducerConfigTests(unittest.TestCase):
    def test_waits_for_every_in_sync_copy(self):
        self.assertEqual(producer_config()['acks'], 'all')

    def test_retries_cannot_write_duplicates(self):
        self.assertIs(producer_config()['enable.idempotence'], True)


class RateTests(unittest.TestCase):
    def test_divides_rows_by_seconds(self):
        self.assertEqual(rows_per_second(500, 2.0), 250.0)

    def test_zero_seconds_gives_zero(self):
        self.assertEqual(rows_per_second(500, 0), 0)


class PeakMemoryTests(unittest.TestCase):
    def test_reports_a_plausible_number_of_megabytes(self):
        self.assertTrue(1 < peak_memory_mb() < 100_000)


if __name__ == '__main__':
    unittest.main()
