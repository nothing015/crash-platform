"""What the consumer has to remember: the counts and how far it has read, saved as one unit.

Saving them together is what makes a restart safe. The offset in a checkpoint is always the
first message whose effect is not in that checkpoint's counts, so a restart can neither lose
a message nor count one twice.
"""
import json
from collections import Counter, defaultdict

from common import borough_of, hour_key, write_json

BOROUGHS = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'UNKNOWN']


class StreamState:
    def __init__(self):
        self.counts = defaultdict(Counter)      # counts[hour][borough] = crashes
        self.stats = {'messages': 0, 'bad': 0, 'unknown_borough': 0, 'first_ts': None, 'last_ts': None}
        self.offsets = {}                       # offsets[partition] = next offset to read
        self.current_hour = None                # the latest hour seen so far

    def apply(self, partition, offset, value):
        """Count one message. Returns its contents, or None if it was skipped or unreadable."""
        if offset < self.offsets.get(partition, 0):
            return None                         # already counted: safe to deliver twice
        self.offsets[partition] = offset + 1
        try:
            data = json.loads(value)
            hour = hour_key(data['crash_ts'])
            borough = borough_of(data.get('borough'))
        except (ValueError, KeyError, TypeError, AttributeError):
            self.stats['bad'] += 1
            return None

        self.counts[hour][borough] += 1
        if self.current_hour is None or hour > self.current_hour:
            self.current_hour = hour            # a late message never moves this back
        self.stats['messages'] += 1
        self.stats['unknown_borough'] += borough == 'UNKNOWN'
        self.stats['first_ts'] = self.stats['first_ts'] or data['crash_ts']
        self.stats['last_ts'] = data['crash_ts']
        return data


def save_checkpoint(path, state, topic_id):
    """Save counts and offsets in one atomic write. A crash leaves the previous checkpoint intact."""
    write_json(path, {
        'topic_id': topic_id,
        'offsets': state.offsets,
        'current_hour': state.current_hour,
        'stats': state.stats,
        'counts': state.counts,
    }, indent=None)


def load_checkpoint(path, topic_id):
    """Return the saved state, or None if there is none or it belongs to an older topic of this name."""
    try:
        with open(path) as f:
            saved = json.load(f)
    except FileNotFoundError:
        return None
    if saved['topic_id'] != topic_id:
        return None
    state = StreamState()
    state.offsets = {int(partition): offset for partition, offset in saved['offsets'].items()}
    state.current_hour = saved['current_hour']
    state.stats = saved['stats']
    for hour, boroughs in saved['counts'].items():
        state.counts[hour] = Counter(boroughs)
    return state


def latest_hour_rows(state):
    """The answer to question 3: (hour, borough, crashes) for the latest hour, with quiet boroughs as 0."""
    if state.current_hour is None:
        return []
    counter = state.counts[state.current_hour]
    hour = state.current_hour + ':00:00'
    return [(hour, borough, counter[borough]) for borough in sorted(set(BOROUGHS) | set(counter))]
