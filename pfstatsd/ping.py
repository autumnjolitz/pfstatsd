import asyncio
import collections
import itertools
import logging
import math
import random
import socket
import time
from enum import Enum
from ipaddress import ip_address

import aiodns


logger = logging.getLogger(__name__)

class PingPreamble(collections.namedtuple('PingPreamble', 'host')):
    __slots__ = ()
    def __new__(cls, host):
        if isinstance(host, bytes):
            host = host.decode('utf8')
        return super().__new__(cls, host)

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


async def ping(host, resolver: aiodns.DNSResolver=None, resolve_ttl=100, socket_type=socket.AF_INET, exit_after=None):
    '''
    :resolve_ttl: int: if we resolve a list of ip addresses, how many pings should we
        try before switching ip address/re-resolve it? If you specify a raw ip,
        we will not ever re-resolve it.
    '''
    assert exit_after is None or isinstance(exit_after, ExitAfterPolicy)
    assert resolve_ttl > 0 or resolve_ttl == -1
    ping_command = 'ping'
    if socket_type == socket.AF_INET6:
        ping_command = 'ping6'

    program_arguments = lambda ip: (ping_command, ip)
    try:
        ips = [ip_address(host)]
        ip_ttl = -1
    except ValueError:
        if resolver is None:
            raise ValueError('you must specify an aiodns.DNSResolver!')
        result = await resolver.gethostbyname(host, socket_type)
        logger.debug('Resolved {} -> {} (up to 10)'.format(host, result.addresses[:10]))
        ips = [ip_address(x) for x in result.addresses]
        if resolve_ttl > 0:
            program_arguments = lambda ip: (ping_command, '-c', str(resolve_ttl), ip)

    packet_count = 0
    t_s = time.time()
    while exit_after is None or not exit_after.poll(time.time() - t_s, packet_count):
        ip = str(random.choice(ips))
        last_seq_index = None
        program_exited = False

        ping_handle = await asyncio.create_subprocess_exec(
            *program_arguments(ip),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        while (exit_after is None or not exit_after.poll(time.time() - t_s, packet_count)) and \
                not program_exited:
            ready, pending = await asyncio.wait(
                (ping_handle.stdout.readline(), ping_handle.stderr.readline()),
                return_when=asyncio.FIRST_COMPLETED)
            for p in pending:
                p.cancel()

            for future in ready:
                line = future.result()
                program_exited = line == b''

                try:
                    packet = parse_line(line)
                    if packet is None:
                        # Garbage line...
                        continue
                    if isinstance(packet, PingPreamble):
                        logger.debug(f'Starting ping to {ip}')
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
                except Exception:
                    logger.exception(f'Unable to parse {line!r}')
        if not program_exited:
            ping_handle.terminate()
        await ping_handle.wait()


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
                values['packet_size_bytes'] = values.pop('packet_size')
                values['time_ms'] = to_ms(*values.pop('time').split(' '))
                del values['_']
            break
        buf.append(char)
    return ICMPResponse(**values)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('destination', type=str)
    parser.add_argument('-t', '--time-limit', default=None, type=float)
    parser.add_argument('-c', '--count', default=None, type=int)
    parser.add_argument('-d', '--debug', default=logging.INFO, action='store_const', const=logging.DEBUG)

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
    resolver = aiodns.DNSResolver(loop=loop)
    loss_count = 0
    count = 0
    async def main():
        global loss_count
        global count
        async for packet in ping(args.destination, resolver, exit_after=exit_policy):
            print(packet)
            count += 1
            if packet.lost:
                loss_count += 1
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print('{} -> {} sent -> {} lost -> loss rate: {}'.format(
            args.destination, count, loss_count, loss_count/count))
    finally:
        loop.close()

