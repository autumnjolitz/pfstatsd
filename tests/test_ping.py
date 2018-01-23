from pfstatsd.ping import parse_line, ICMPResponse, ping, ExitAfterPolicy, Unit
import asyncio
import pytest
import time


def test_parse_line():
    base = ICMPResponse(host='216.58.195.78', packet_size_bytes=64, icmp_seq=-1, time_ms=0, ttl=53)
    assert parse_line(b'64 bytes from 216.58.195.78: icmp_seq=0 ttl=53 time=36.935 ms') == \
        base._replace(icmp_seq=0, time_ms=36.935)
    assert parse_line(b'64 bytes from 216.58.195.78: icmp_seq=1 ttl=53 time=37.304 ms') == \
        base._replace(icmp_seq=1, time_ms=37.304)
    assert parse_line(b'64 bytes from 216.58.195.78: icmp_seq=2 ttl=53 time=55.811 ms') == \
        base._replace(icmp_seq=2, time_ms=55.811)
    assert parse_line(b'64 bytes from 216.58.195.78: icmp_seq=3 ttl=53 time=55.555 ms') == \
        base._replace(icmp_seq=3, time_ms=55.555)
    assert parse_line(b'64 bytes from 216.58.195.78: icmp_seq=4 ttl=53 time=149.834 ms') == \
        base._replace(icmp_seq=4, time_ms=149.834)
    assert parse_line(b'Request timeout for icmp_seq 23') == \
        ICMPResponse(host=None, packet_size_bytes=None, icmp_seq=23, time_ms=float('inf'), ttl=0)


@pytest.fixture(scope='module')
def event_loop():
    return asyncio.get_event_loop()


@pytest.mark.asyncio
async def test_ping(event_loop):
    packets = []
    async for packet in ping('127.0.0.1', exit_after=ExitAfterPolicy(5, Unit.Packets)):
        packets.append(packet)
    assert len(packets) == 5
    t_s = time.time()
    packets[:] = []
    async for packet in ping('127.0.0.1', exit_after=ExitAfterPolicy(5, Unit.Seconds)):
        packets.append(packet)
    assert 5 <= time.time() - t_s <= 10
    assert 4 <= len(packets) <= 10
