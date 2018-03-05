import asyncio
import logging
import pickle
import struct
import time
import errno
from typing import Optional, Union

from . import DEFAULT_STDOUT_FORMAT
from .protocols import ProtocolStateMachine


logger = logging.getLogger(__name__)

Seconds = int
Length = int


class Session:
    def __init__(self, host, port=2004, namespace='', queue_max: Length=100,
                 delay_max: Seconds=10, **kwargs):
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
    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.transport = None
        self._wait_for_connections = asyncio.Event(loop=loop)
        self._trigger_reconnect = asyncio.Event(loop=loop)
        self._retry_future = None
        self._flush_before_connect = None
        self._initial_connect = None

    def using(self, namespace: str, join=False, *, conn=False, loop=None, **kwargs):
        client = super().using(namespace, join, **kwargs)
        logger.debug('spawning subclient')
        if conn:
            logger.debug('... with connection starting')
            client._initial_connect = asyncio.ensure_future(
                client.connect(initial=True), loop=loop)
        return client

    async def _reconnect(self, *, loop=None):
        await self._wait_for_connections.wait()
        await self._trigger_reconnect.wait()

        logger.debug('reconnect code primed')
        logger.debug('Invoking reconnect code')
        self._retry_future = None
        self._trigger_reconnect.clear()
        return await self.connect(loop=loop)

    async def connect(self, *, loop=None, initial=False):
        if self.current_state not in ('not_connected', 'connection_lost', 'eof_received'):
            logger.debug('Already connected')
            return self

        if self._initial_connect and not initial:
            logger.debug('Waiting on outstanding connect()')
            return await self._initial_connect

        if loop is None:
            loop = asyncio.get_event_loop()
        logger.debug('Making connection to graphite')
        index = 0
        while True:
            try:
                await loop.create_connection(lambda: self, self.host, self.port)
                break
            except (ConnectionError, OSError) as e:
                if isinstance(e, OSError) and str(e).startswith('Multiple exceptions: '):
                    errnos = set()
                    for error in str(e)[len('Multiple exceptions: '):].split(','):
                        if error.startswith('[Errno'):
                            code = int(error[len('[Errno '):error.index(']')])
                            errnos.add(code)
                    if not (errnos & frozenset(
                            (errno.ECONNREFUSED, errno.ECONNABORTED, errno.ECONNRESET))):
                        raise

                delay = 2 ** index
                logger.error(
                    f'Unable to connect to {self.host}:{self.port}, retrying in {delay:.2f}s')
                await asyncio.sleep(delay)
                index += 1
                if index > 3:
                    index = 0

        logger.debug('Making connection to graphite [done]')
        if self._initial_connect:
            self._initial_connect = None

        prior_future = self._retry_future
        self._retry_future = asyncio.ensure_future(self._reconnect(loop=loop))
        if prior_future:
            prior_future.cancel()
        return self

    async def close(self):
        if self._retry_future:
            self._retry_future.cancel()
            self._retry_future = None
        if self._flush_before_connect:
            self._flush_before_connect.cancel()
            self._flush_before_connect = None

        if self.transport is not None:
            self.transport.close()

        self.current_state = 'not_connected'
        self.transport = None
        self._wait_for_connections.clear()
        self._trigger_reconnect.clear()

    def connection_made(self, transport):
        if self.transport is not None:
            logger.warn('multiple connect() called, duplicate transports found, closing extras')
            transport.close()
            return
        logger.debug('Connection established to {}'.format(transport))
        super().connection_made(transport)
        self.transport = transport
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

    async def flush(self, blocking=False, *, loop=None):
        length = len(self.queue)
        if not length:
            return 0

        if blocking:
            return await self._deferred_flush()

        if self._flush_before_connect:
            return 0
        if not self._wait_for_connections.is_set():
            self._flush_before_connect = asyncio.ensure_future(self._deferred_flush(), loop=loop)
            return 0
        return self._flush()

    async def _deferred_flush(self):
        '''Block until the connection is up.

        This is used as a way to create a single task in the event loop responsible
        for making sure paused metrics will be sent when connection is re-established.
        '''
        try:
            await self._wait_for_connections.wait()
            sent = self._flush()
        except Exception:
            logger.exception('Unexpected error in deferred flush')
            raise
        finally:
            self._flush_before_connect = None
        logger.info(f'Sent {sent} blocked metrics!')
        return sent

    def _flush(self):
        length = len(self.queue)
        now = time.time()
        items = self.queue[:length]
        payload = pickle.dumps(items, protocol=2)
        self.queue[:] = self.queue[length:]

        header = struct.pack("!L", len(payload))
        message = header + payload
        try:
            self.transport.write(message)
        except IOError:
            self.queue.extend(items)
            raise
        del items
        self.last_flush_ts = now
        return length


if __name__ == '__main__':
    import argparse
    from . import parse_host
    from . import uvloop

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
