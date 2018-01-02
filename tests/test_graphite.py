import random
import asyncio
import pytest
import collections
import socket
import struct
import pickle
import time

from pfstatsd import parse_host
from pfstatsd.graphite import Session


class Server:
    def __init__(self, socket, port):
        self.socket = socket
        self.port = port
        self._read_socket = None

    def __iter__(self):
        yield self.socket
        yield self.port

    @property
    def read_socket(self):
        if not self._read_socket:
            read_socket = self.socket
            if self.socket.type == socket.SocketKind.SOCK_STREAM:
                read_socket, _ = self.socket.accept()
            self._read_socket = read_socket
        return self._read_socket


TimeStampedValue = collections.namedtuple('TimeStampedValue', ['timestamp', 'raw_value'])


class Metric(collections.namedtuple('Metric', ['key', 'value'])):
    __slots__ = ()

    def __new__(cls, key, value):
        if not isinstance(value, TimeStampedValue):
            value = TimeStampedValue(*value)
        return super().__new__(cls, key, value)

    @property
    def raw_value(self):
        return self.value.raw_value

    @property
    def timestamp(self):
        return self.value.timestamp

sizeof_signed_long = len(struct.pack('!L', 1))


@pytest.fixture(scope='module')
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='module')
async def client(server):
    _, port = server
    session = Session('localhost', port)
    session.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # session.conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65535)
    session.conn.connect(('localhost', port))
    return session


@pytest.fixture(scope='module')
def server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65535)
    server_socket.bind((b'localhost', 0))
    server_socket.listen(10)
    server_port = server_socket.getsockname()[1]
    return Server(server_socket, server_port)


async def parse_metrics(event_loop, server):
    data = bytearray()
    message_length = None

    result_bytes = await event_loop.sock_recv(server.read_socket, 2048)
    while result_bytes:
        data.extend(result_bytes)
        if message_length is None and len(data) >= sizeof_signed_long:
            message_body_length, = struct.unpack('!L', data[:sizeof_signed_long])
            message_length = message_body_length + sizeof_signed_long
        assert message_length - len(data) > -1
        if message_length == len(data):
            break
        result_bytes = await event_loop.sock_recv(
            server.read_socket, message_length - len(data) if message_length - len(data) < 2048 else 2048)

    assert result_bytes, 'Parsed empty data from server socket?'
    message_body = memoryview(data)[sizeof_signed_long:]
    return [Metric(*metric) for metric in pickle.loads(message_body)]


@pytest.mark.asyncio
async def test_simple_post(event_loop, server, client):
    client._append_metric('key', 1, 123, '')
    await client.flush()
    metric, = await parse_metrics(event_loop, server)
    assert metric.key == 'key'
    assert metric.raw_value == 1
    assert metric.timestamp == 123

@pytest.mark.asyncio
async def test_queue_flushing(event_loop, server, client):
    client = client.using('test_namespace', conn=client.conn)
    client.queue_max = 100
    client.delay_max = -1
    keys = ('abc', 'def', 'hij', 'kml')
    futures = []
    expected_results = set()
    count = 1000
    for _ in range(count):
        t_s = time.time()
        key = f'key-{random.choice(keys)}'
        value = random.randint(1, 23412)
        expected_results.add(Metric(f'test_namespace.{key}', (t_s, value)))
        futures.append(client.post(key, value, t_s))
    num_sent = sum(await asyncio.gather(*futures))
    assert num_sent == count
    metrics = []
    while len(metrics) < count:
        metrics.extend(await parse_metrics(event_loop, server))
    assert frozenset(metrics) == expected_results



def test_parse_host():
    host, port = parse_host('foobar.com:2004', -1)
    assert host == 'foobar.com'
    assert port == 2004


def test_parse_ipv6():
    host, port = parse_host('[::1]:2004', -1)
    assert host == '::1'
    assert port == 2004
    host, port = parse_host('[::1]', 8012)
    assert host == '::1'
    assert port == 8012
    with pytest.raises(ValueError) as e:
        host, port = parse_host('::1', 8012)
    assert 'brackets' in str(e)


def test_default_port():
    host, port = parse_host('localhost', 2004)
    assert host == 'localhost'
    assert port == 2004
