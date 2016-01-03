
import pandas
import random
import sys
import uuid


COLUMNS = ['vcpus', 'ram', 'disk']
COL_ORDER = ['ram', 'vcpus', 'disk']


def _host_state(node_id):
    return {
        'id': node_id,
        'ram': int(random.random() * 1024) + 1,
        'disk': int(random.random() * 1024) + 1,
        'vcpus': int(random.random() * 10) + 1,
    }


def _usage():
    return {
        'ram': (int(random.random() * 10) + 1),
        'disk': (int(random.random() * 10) + 1),
        'vcpus': (int(random.random() * 4) + 1),
    }


def make_data():
    """Make some random chunks of host data."""
    data = []
    for node_id in range(10):
        data.append(_host_state(str(uuid.uuid4())))
    return data


def update_resources(frame, node_id):
    print('### Updating %s' % node_id)
    frame.loc[node_id] = _host_state(node_id)


def consume_resources(frame):
    frame = sort_frame(frame)
    usage_series = pandas.Series(_usage(), index=COLUMNS)
    match_test = ((frame.vcpus >= usage_series.vcpus) &
                  (frame.ram >= usage_series.ram) &
                  (frame.disk >= usage_series.disk))
    match = frame[match_test].head(1)

    if not match.empty:
        frame.ix[match.index] -= usage_series
        return match.index.values, frame
    else:
        print '.',
        return None, frame


def sort_frame(frame):
    greater_zero = ((frame.vcpus > 0) & (frame.ram > 0) & (frame.disk > 0))
    frame = frame[greater_zero].sort_values(by=COL_ORDER, ascending=False)
    return frame


def start(requests):
    frame = pandas.DataFrame(
        [],
        columns=COLUMNS,
    )

    for item in make_data():
        series = pandas.Series(item, index=COLUMNS)
        frame.loc[item['id']] = series

    destinations = []
    tries = 0
    while requests > len(destinations) and tries < (requests * 5):
        index, frame = consume_resources(frame)
        if index is not None:
            destinations.append(index)
        else:
            tries += 1

    print '\ndestinations %s\n%s of %s' % (
        destinations, len(destinations), requests
    )

if __name__ == '__main__':
    start(int(sys.argv[1]))
