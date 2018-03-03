import logging
import time
import asyncio
import aiodns
import yaml

from . import DEFAULT_STDOUT_FORMAT, parse_host
from .pf import stream_queue_status
from .graphite import Session
from .ping import ping, ICMPResponse, AbnormalExit, ExitAfterPolicy, Unit

logger = logging.getLogger('pfstatsd')

async def monitor_pf_queue(session, duration=-1, delay=0.5):
    '''
    Monitor the queue status, send metrics to host:port
    '''
    t_s = time.time()
    num_sent = 0
    session = session.using('pf', join=True)
    await session.connect()
    logger.info(f'Reading queue status, duration limit set to {duration}')

    async for queue_status in stream_queue_status():
        for queue_name, queue_data in queue_status.items():
            # write the edge most queues to a flat key:
            for metric_name, value in queue_data.metrics.items():
                num_sent += await session.post(f'{queue_name}.{metric_name}', value)
        logger.debug(f'Sent {num_sent} metrics to graphite')
        await asyncio.sleep(delay)
        if duration > 0 and time.time() - t_s > duration:
            break
    logger.debug('Done monitoring PF')


async def monitor_remote_icmp(session, host, policy, resolver):
    session = session.using('ping.{}'.format(host.replace('.', '-')), join=True)
    await session.connect()
    num_sent = 0
    packets_seen = 0
    packets_lost = 0
    try:
        async for packet in ping(host, policy, resolver=resolver):
            logger.debug(f'{host}->{packet!s}, {num_sent} sent so far')
            if isinstance(packet, ICMPResponse):
                packets_seen += 1
                num_sent += await session.post('packets.sent', packets_seen)
                if packet.lost:
                    packets_lost += 1
                    num_sent += await session.post('packets.lost', packets_lost)
                    continue
                num_sent += await session.post('latency_ms', packet.time_ms)
                num_sent += await session.post('packets.recv', packets_seen)
    except AbnormalExit as e:
        logger.exception(f'Unexpected exit for {host}, code {e.code}')
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception('wtf')
        raise
    else:
        logger.debug(f'Done with {host}')

async def main(host, port, duration=-1, namespace='', *icmp_hosts):
    session = Session(host, port, delay_max=1, namespace=namespace)
    if icmp_hosts:
        resolver = aiodns.DNSResolver()
        policy = None
        if duration > 0:
            policy = ExitAfterPolicy(duration, Unit.Seconds)
    pf_status = asyncio.ensure_future(monitor_pf_queue(session, duration))
    done, pending = await asyncio.wait(
        [pf_status] +
        [monitor_remote_icmp(session, host, policy, resolver) for host in icmp_hosts],
        return_when=asyncio.FIRST_EXCEPTION)
    if pf_status in done and pf_status.exception() is not None:
        for future in pending:
            future.cancel()
        logger.exception('Could not gather PF information, fatal', exc_info=pf_status.exception())
        raise SystemExit(1)

    if pending:
        remainder, pending = await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
        assert not pending
        for future in done | remainder:
            try:
                logger.debug('{} -> {}'.format(future, future.result()))
            except Exception:
                logger.warn(f'{future} threw an uncaught error')


if __name__ == '__main__':
    from contextlib import closing
    import argparse
    parser = argparse.ArgumentParser(prog='pfstatsd')
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('--sudo', action='store_true', default=False, dest='use_sudo')

    subparsers = parser.add_subparsers()
    config_parser = subparsers.add_parser('from')
    config_parser.add_argument('config_file', type=argparse.FileType('rb'), help='config file')
    config_parser.add_argument(
        '-t', '--time-limit', type=float, default=-1, help='Limit on how long this runs')

    run_parser = subparsers.add_parser('run')
    run_parser.add_argument(
        '-n', '--namespace', type=str, help='graphite namespace to write to, defautls to \'\'',
        default='')
    run_parser.add_argument(
        '-t', '--time-limit', type=float, default=-1, help='Limit on how long this runs')
    run_parser.add_argument('host', help='graphite host:[port] combo')
    run_parser.add_argument('remote_hosts', help='hosts to ping', nargs='+')

    args = parser.parse_args()

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(DEFAULT_STDOUT_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        with closing(args.config_file) as fh:
            config = yaml.load(fh)
        host, port = config['graphite']
    except AttributeError:
        host, port = parse_host(args.host, default_port=2004)
    else:
        if config.get('use_sudo'):
            args.use_sudo = True
        host, port = config['graphite']['host'], config['graphite']['port']
        if 'time_limit' in config:
            args.time_limit = config['time_limit']
        args.namespace = config.get('namespace', '')
        args.remote_hosts = config['remote_hosts']
    if args.use_sudo:
        from . import pf
        pf.READ_QUEUE_STATUS = f'sudo {pf.READ_QUEUE_STATUS}'

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(
        host, port, args.time_limit, args.namespace, *args.remote_hosts))
