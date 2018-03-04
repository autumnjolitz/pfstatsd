import asyncio
import logging
import os
import pickle
import socket
import struct
import time
from typing import Optional, Union

from . import DEFAULT_STDOUT_FORMAT
from . import uvloop
from .protocols import ProtocolStateMachine


logger = logging.getLogger(__name__)

Seconds = int
Length = int


class Session:
    def __init__(self, host, port=2004, namespace='', queue_max: Length=100, delay_max: Seconds=10):
        assert isinstance(port, int) and port > 0
        assert host and isinstance(host, str)
        assert isinstance(queue_max, int) and queue_max > 0 or queue_max == -1, \
            'Non-zero queue limit or -1 to disable'
        assert delay_max > 0 or delay_max == -1, 'Non-zero delays required or -1 to disable'
        assert all(char in ('.', '_', '-') or char.isalnum() for char in namespace), \
            f'Namespaces ({namespace!r}) must be in regex of [A-Za-z0-9\.]'

        self.host = host
        self.port = port
        self.queue = []

        self.queue_max = queue_max
        self.delay_max = delay_max
        self.namespace = namespace
        self.last_flush_ts = -1

    def using(self, namespace: str, join=False, **kwargs):
        new_cls_kwargs = {
            'host': self.host,
            'port': self.port,
            'namespace': namespace,
            'queue_max': self.queue_max,
            'delay_max': self.delay_max
        }
        new_cls_kwargs.update({
            key: kwargs[key] for key in kwargs.keys() & new_cls_kwargs.keys()})
        if join and self.namespace:
            new_cls_kwargs['namespace'] = f'{self.namespace}.{new_cls_kwargs["namespace"]}'
        return self.__class__(**new_cls_kwargs)

    def _append_metric(self, name, value, timestamp, namespace):
        assert ' ' not in name, 'spaces not allowed'
        assert name, 'need a name'
        assert isinstance(value, (int, float)), 'Must be a numeric value!'

        if namespace is None:
            namespace = self.namespace
        if namespace:
            name = f'{namespace}.{name}'
        self.queue.append((name, (timestamp or time.time(), value,)))


    async def post(self, name: str, value: Union[int, float],
                   timestamp: Optional[Union[int, float]]=None, namespace: Optional[str]=None,
                   *, loop=None):
        '''
        Post to the metric at ``name`` with value. Allow for custom
        timestamp (defaults to ``time.time()``)
        '''
        now = time.time()
        num_sent = 0
        self._append_metric(name, value, timestamp, namespace)
        if self.delay_max != -1 and now - self.last_flush_ts >= self.delay_max:
            num_sent = await self.flush(loop=loop)
        elif self.queue_max != -1 and len(self.queue) >= self.queue_max:
            num_sent = await self.flush(loop=loop)
        return num_sent


class TCPGraphite(ProtocolStateMachine, Session, asyncio.Protocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transport = None
        self._wait_for_connections = asyncio.Event()
        self._trigger_reconnect = asyncio.Event()

    def using(self, namespace: str, join=False, *, conn=False, **kwargs):
        client = super().using(namespace, join, **kwargs)

        if conn:
            assert self.transport
            client.transport = self.transport
            client._wait_for_connections.set()
            client._trigger_reconnect.clear()

            future = asyncio.ensure_future(self.connect(loop=None))
        return client

    async def connect(self, *, loop=None):
        if self._wait_for_connections.is_set():
            logger.debug('reconnect code primed')
            await self._trigger_reconnect.wait()
            self._trigger_reconnect.clear()
            logger.debug('Invoking reconnect code')
            return await self.connect(loop=loop)

        if loop is None:
            loop = asyncio.get_event_loop()
        logger.debug('Making connection to graphite')
        await loop.create_connection(lambda: self, self.host, self.port)
        logger.debug('Making connection to graphite [done]')
        future = asyncio.ensure_future(self.connect(loop=loop))
        return self

    async def close(self):
        if self.transport is None:
            return
        self.transport.close()
        self.transport = None

    def connection_made(self, transport):
        super().connection_made(transport)
        self.transport = transport
        logger.debug('Connection established to {}'.format(transport))
        self._wait_for_connections.set()

    def data_received(self, data):
        super().data_received(data)
        logger.debug(f'Wasted data {data}')

    def eof_received(self):
        '''
        Called when graphite is restarted, etc.

        This should indicate reconnection.
        '''
        super().eof_received()
        logger.debug(f'EOF received from {self.host}:{self.port}')
        self._wait_for_connections.clear()
        self._trigger_reconnect.set()
        self.transport = None

    def connection_lost(self, exc):
        super().connection_lost(exc)

        if self._wait_for_connections.is_set():
            logger.debug('Connection was lost before an EOF appeared.')
            self._trigger_reconnect.set()
            self._wait_for_connections.clear()

        if exc is not None:
            logger.exception(
                f'Graphite({self.host}:{self.port}) unexpectedly disconnected', exc_info=exc)
        self.transport = None

    async def flush(self, loop=None):
        await self._wait_for_connections.wait()

        now = time.time()
        length = len(self.queue)
        if not length:
            return 0

        if loop is None:
            loop = asyncio.get_event_loop()
        payload = pickle.dumps(self.queue[:length], protocol=2)
        self.queue[:] = self.queue[length:]

        header = struct.pack("!L", len(payload))
        message = header + payload
        self.transport.write(message)
        self.last_flush_ts = now
        return length


if __name__ == '__main__':
    import argparse
    from . import parse_host

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('host', help='hostname:[port] to send to, defaults to port 2004')
    parser.add_argument('key', help='metric key')
    parser.add_argument('value')
    parser.add_argument('timestamp', default=None, type=float, nargs='?')

    args = parser.parse_args()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(DEFAULT_STDOUT_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        args.value = int(args.value)
    except ValueError:
        args.value = float(args.value)

    host, port = parse_host(args.host, default_port=2004)

    client = TCPGraphite(host, port)
    async def main(client, args):
        logger.debug(f'Connecting to {client.host}:{client.port}')
        await client.connect()
        logger.debug(f'Posting {args.key} -> {args.value} (@{args.timestamp})')
        await client.post(args.key, args.value, args.timestamp)
        logger.debug('Flushing')
        await client.flush()
        logger.debug('Done')

    uvloop and logger.debug('Using uvloop')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(client, args))
