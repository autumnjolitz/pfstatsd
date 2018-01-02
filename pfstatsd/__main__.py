import argparse
import logging
import time
import asyncio

from . import DEFAULT_STDOUT_FORMAT, parse_host
from .pf import read_queue_status
from .graphite import Session

logger = logging.getLogger('pfstatsd')

async def main(host, port, namespace, duration=-1):
    session = Session(host, port, namespace)
    await session.connect()

    logger.info(f'Reading queue status, duration limit set to {duration}')
    while True:
        num_sent = 0
        queues = await read_queue_status()
        queues = {key: value for key, value in queues.items() if not value['children']}
        for queue_name, queue_data in queues.items():
            # write the edge most queues to a flat key:
            for metric_name, value in queue_data.metrics.items():
                num_sent += await session.post(f'{queue_name}.{metric_name}', value)
        num_sent += await session.flush()
        logger.debug(f'Sent {num_sent} metrics to graphite')
        await asyncio.sleep(1)
        elapsed = time.time() - t_s
        if duration > 0 and elapsed >= duration:
            logger.info('Exiting after {:.2f} seconds'.format(elapsed))
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='pfstatsd')
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('-t', '--time-limit', type=float, default=-1, help='Limit on how long this runs')
    parser.add_argument('host', help='graphite host:[port] combo')
    parser.add_argument('namespace', default='pf', type=str, help='Destination ns of graphite', nargs='?')

    args = parser.parse_args()

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(DEFAULT_STDOUT_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    host, port = parse_host(args.host, default_port=2004)

    t_s = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(host, port, args.namespace, args.time_limit))
