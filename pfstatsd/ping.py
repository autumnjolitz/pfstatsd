import asyncio
import collections
import itertools
import logging
import math
import random
import signal
import socket
import time
from enum import Enum
from ipaddress import ip_address, IPv4Address, IPv6Address

import aiodns

from . import AbnormalExit


logger = logging.getLogger(__name__)

class PingPreamble(collections.namedtuple('PingPreamble', ('ip', 'host'))):
    __slots__ = ()

    def __new__(cls, host, ip=None):
        if isinstance(host, bytes):
            host = host.decode('utf8')
        return super().__new__(cls, ip or host, host)

    def __str__(self):
        return f'PING {self.host} ({self.ip}): 56 data bytes'


class ICMPResponse(collections.namedtuple(
        'ICMPResponse', ['host', 'time_ms', 'icmp_seq', 'ttl', 'packet_size_bytes'])):
    __slots__ = ()

    def __new__(cls, host, time_ms, icmp_seq, ttl, packet_size_bytes):
        if isinstance(host, bytes):
            host = host.decode('utf8')
        if not isinstance(icmp_seq, int):
            icmp_seq = int(icmp_seq, 10)
        if not isinstance(ttl, int):
            ttl = int(ttl, 10)
        if packet_size_bytes and not isinstance(packet_size_bytes, int):
            packet_size_bytes = int(packet_size_bytes, 10)
        return super().__new__(cls, host, time_ms, icmp_seq, ttl, packet_size_bytes)

    @property
    def lost(self):
        return self.host is None or math.isinf(self.time_ms)

    def __str__(self):
        return f'{self.packet_size_bytes} bytes from {self.host}: ' \
               f'icmp_seq={self.icmp_seq} ttl={self.ttl} time={self.time_ms} ms'


class Unit(Enum):
    Seconds = 1
    Packets = 2


class ExitAfterPolicy(object):

    __slots__ = ('value', 'unit', 'other_policies')

    def __init__(self, value, unit, other_policies=None):
        assert isinstance(value, (int, float))
        assert isinstance(unit, Unit)
        self.value = value
        self.unit = unit
        self.other_policies = other_policies or ()

    def _poll(self, time_elapsed, num_packets):
        if self.unit == Unit.Packets:
            return num_packets >= self.value
        elif self.unit == Unit.Seconds:
            return time_elapsed >= self.value
        raise NotImplementedError

    def poll(self, time_elapsed, num_packets):
        return all(
            policy._poll(time_elapsed, num_packets)
            for policy in itertools.chain((self,), self.other_policies))

    def __or__(self, other):
        assert isinstance(other, self.__class__)
        return self.__class__(self.value, self.unit, other_policies=self.other_policies + (other,))

async def random_resolve(host, resolver: aiodns.DNSResolver=None, *,
                         sock_types=(socket.AF_INET, socket.AF_INET6), loop=None):
    try:
        return ip_address(host)
    except ValueError:
        resolver = resolver or aiodns.DNSResolver(loop=loop or asyncio.get_event_loop())
        for sock_type in sock_types:
            try:
                result = await resolver.gethostbyname(host, sock_type)
                ips = result.addresses
            except aiodns.error.DNSError:
                continue
            else:
                if not ips:
                    continue
                return ip_address(random.choice(ips))
        raise ValueError(f'Unable to get an ip address for {host}')


async def ping(host, exit_after=None,
               resolver: aiodns.DNSResolver=None, sock_types=(socket.AF_INET, socket.AF_INET6),
               *, loop=None):
    '''
    :resolve_ttl: int: if we resolve a list of ip addresses, how many pings should we
        try before switching ip address/re-resolve it? If you specify a raw ip,
        we will not ever re-resolve it.
    '''
    assert exit_after is None or isinstance(exit_after, ExitAfterPolicy)
    assert isinstance(host, (str, IPv4Address, IPv6Address))
    ip = host
    if isinstance(host, str):
        ip = await random_resolve(host, resolver=resolver, sock_types=sock_types, loop=loop)

    ping_command = 'ping'
    if isinstance(host, IPv6Address):
        ping_command = 'ping6'

    packet_count = 0
    t_s = time.time()
    last_seq_index = None
    program_exited = False
    try:
        ping_handle = await asyncio.create_subprocess_exec(
            ping_command, str(host),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        should_exit = lambda *args: False
        if exit_after:
            should_exit = exit_after.poll
        while not should_exit(time.time() - t_s, packet_count):
            ready, pending = await asyncio.wait(
                (ping_handle.stderr.readline(), ping_handle.stdout.readline(),),
                return_when=asyncio.FIRST_COMPLETED)
            for p in pending:
                p.cancel()

            for future in ready:
                line = future.result()
                program_exited = line == b''

                try:
                    packet = parse_line(line)
                except Exception:
                    logger.exception(f'Unable to parse {line!r}')
                else:
                    if packet is None:
                        # Garbage line...
                        continue
                    if isinstance(packet, PingPreamble):
                        yield packet._replace(host=host)
                        continue
                    if packet.lost:
                        packet = packet._replace(host=ip)
                    if last_seq_index is not None and packet.icmp_seq - last_seq_index > 1:
                        logger.debug('Detected {} lost packets!'.format(
                            packet.icmp_seq - last_seq_index - 1))
                        for lost_packet_index in range(last_seq_index+1, packet.icmp_seq):
                            yield ICMPResponse(ip, float('inf'), lost_packet_index, 0, None)
                    yield packet
                    packet_count += 1
                    last_seq_index = packet.icmp_seq
            if program_exited:
                break
        if ping_handle.returncode is None:
            ping_handle.send_signal(signal.SIGINT)
            await ping_handle.wait()
        if ping_handle.returncode:
            raise AbnormalExit(ping_handle.returncode, 'Non-zero exit code')
        ping_handle = None
    except asyncio.CancelledError:
        if ping_handle:
            ping_handle.terminate()
            await ping_handle.wait()
        raise

class ParseMode(Enum):
    VALUE = 1
    KEY = 2


def to_ms(value, unit):
    assert unit in ('ms', 's'), b'unsupported time unit {unit!r}'
    if unit == 's':
        return float(value) * 1000.
    return float(value)


def parse_line(line: bytes):
    line = line.strip()

    if not line:
        return None

    if line.startswith(b'Request timeout'):
        _, key, value = line.rsplit(b' ', 2)
        assert key == b'icmp_seq'
        return ICMPResponse(None, float('inf'), value, 0, None)
    # PING 127.0.0.1 (127.0.0.1): 56 data bytes\n
    if line.startswith(b'PING '):
        ip, _ = line.rsplit(b':', 1)
        ip = ip[ip.rindex(b'(')+1:-1]
        return PingPreamble(ip)
    values = {}
    buf = bytearray()
    line_view = memoryview(line)
    value = None

    mode = ParseMode.VALUE
    for index, char in enumerate(reversed(line)):
        if char == ord(b' '):
            if mode == ParseMode.KEY:
                mode = ParseMode.VALUE
                key = bytes(memoryview(buf)[::-1]).decode('ascii')
                values[key] = value
                buf[:] = []
                del value
                continue
            buf.append(char)
            continue
        if char == ord(b'='):
            mode = ParseMode.KEY
            value = bytes(memoryview(buf)[::-1]).decode('ascii')
            buf[:] = []
            continue
        if char == ord(':'):
            field_start_index = 0
            field_index = 0
            fields = ('packet_size', 'packet_size_unit', '_', 'host')
            end_index = len(line) - index - 1

            for index, char in enumerate(line_view[:-index]):
                if char == ord(b' ') or index == end_index:
                    values[fields[field_index]] = bytes(line_view[field_start_index:index])
                    field_index += 1
                    field_start_index = index + 1
                    continue
            else:
                packet_size_unit = values.pop('packet_size_unit')
                assert packet_size_unit == b'bytes', f'packet size is {packet_size_unit!r}'
                del packet_size_unit

                values['packet_size_bytes'] = values.pop('packet_size')
                values['time_ms'] = to_ms(*values.pop('time').split(' '))
                del values['_']
            break
        buf.append(char)
    return ICMPResponse(**values)

async def main(host, exit_policy=None):
    resolver = aiodns.DNSResolver()
    loss_count = 0
    count = 0
    try:
        async for packet in ping(host, exit_policy, resolver=resolver):
            logger.info(str(packet))
            if isinstance(packet, ICMPResponse):
                count += 1
                if packet.lost:
                    loss_count += 1
    except asyncio.CancelledError:
        logger.info(
            '--- {} ping statistics ---\n'
            '{} packets transmitted, {} packets received, {:.2f}% packet loss'.format(
                args.destination, count, loss_count, loss_count/count))
        return
    except Exception:
        logger.exception('Uncaught exception')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('destination', type=str)
    parser.add_argument('-t', '--time-limit', default=None, type=float)
    parser.add_argument('-c', '--count', default=None, type=int)
    parser.add_argument('-d', '--debug', default=logging.INFO, action='store_const',
                        const=logging.DEBUG)

    args = parser.parse_args()
    logger.setLevel(args.debug)
    logger.addHandler(logging.StreamHandler())

    policies = []
    exit_policy = None
    if any((args.time_limit, args.count)):
        for item, unit in ((args.time_limit, Unit.Seconds), (args.count, Unit.Packets)):
            if item is not None:
                policies.append(ExitAfterPolicy(item, unit))
        exit_policy = policies[0]
        for item in policies[1:]:
            exit_policy |= item

    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main(args.destination, exit_policy))
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        # Inject the cancellation
        task.cancel()
        # resume the stack
        loop.run_until_complete(task)
        # drain result
        task.result()
    finally:
        loop.close()
