from collections import namedtuple
from ._ifstats import ffi, lib

Sample = namedtuple('Sample', ['row', 'name', 'in_bytes', 'out_bytes', 'timestamp'])


class Timestamp(namedtuple('Timestamp', ['seconds', 'microseconds'])):
    def __sub__(self, other):
        s_diff = self.seconds - other.seconds
        s_udiff = self.microseconds - other.microseconds
        return s_diff + (1e-6 * s_udiff)


def sample(*rows):
    if not rows:
        rows = range(1, lib.get_if_limit())
    for ordinal in rows:
        result = lib.get_stats(ordinal)
        t = Timestamp(result.timestamp.tv_sec, result.timestamp.tv_usec)
        if result.status:
            continue
        yield Sample(ordinal, ffi.string(result.name), result.ibytes, result.obytes, t)
