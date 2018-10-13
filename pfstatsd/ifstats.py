import logging
from collections import namedtuple

from ._ifstats import ffi, lib

logger = logging.getLogger(__name__)


class Sample(namedtuple('Sample', ['row', 'name', 'in_bytes', 'out_bytes', 'timestamp'])):
    def __sub__(self, other):
        if not (self.row == other.row and self.name == other.name):
            raise ValueError('Invalid comparison - row or name differ')
        timespan = self.timestamp - other.timestamp
        deltas = []
        keys = []
        for index in range(2, len(self._fields) - 1):
            deltas.append(self[index] - other[index])
            keys.append(self._fields[index])
        return Rate(keys, tuple(deltas), timespan)

    def list_metrics(self):
        for label, value in zip(self._fields[2:-1], self[2:-1]):
            yield label, value


class Rate(namedtuple('Rate', ['labels', 'deltas', 'timespan'])):
    def as_labeled_rates(self):
        for label, delta in zip(self.labels, self.deltas):
            yield label, delta / self.timespan


class Timestamp(namedtuple('Timestamp', ['seconds', 'microseconds'])):
    def __sub__(self, other):
        s_diff = self.seconds - other.seconds
        s_udiff = self.microseconds - other.microseconds
        return s_diff + (1e-6 * s_udiff)

    def __float__(self):
        return self.seconds + (1e-6 * self.microseconds)


def sample(*rows):
    if not rows:
        rows = range(1, lib.get_if_limit())
    for ordinal in rows:
        result = lib.get_stats(ordinal)
        ts = Timestamp(result.timestamp.tv_sec, result.timestamp.tv_usec)
        if not result.name:
            logger.error(f'{ordinal} gave an empty interface name!')
            continue
        if result.status:
            logger.warn(f'{ordinal} is not accessible')
            continue
        yield Sample(
            ordinal,
            ffi.string(result.name).decode('utf8'),
            result.ibytes, result.obytes, ts)
