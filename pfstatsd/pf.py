import collections
import logging
import asyncio
import subprocess
import shlex
from . import AbnormalExit

logger = logging.getLogger(__name__)
READ_QUEUE_STATUS = 'pfctl -s queue -v'

METRIC_ALIASES = {
    'qlength': 'queue_load_factor'
}


class QueueMetrics(collections.namedtuple('QueueMetrics', ['name', 'children', 'metrics'])):
    __slots__ = ()


def parse_metric(line: str) -> dict:
    has_metric_name = False
    metric_name = None
    has_metric_value = False
    buf = []
    metrics = {}
    prev_char = None
    for char in line[1:-1]:
        if char == ':' and not has_metric_name:
            has_metric_name = True
            # lines often have something like "metric pkts ### bytes ####", so dupe the key name
            if metric_name and metric_name.endswith('pkts'):
                buf.insert(0, metric_name)

                # [key, 'bytes'] -> [key, ' ', 'bytes']
                if buf[1] != ' ':
                    buf.insert(1, ' ')

            metric_name = ''.join(buf).strip().replace(' ', '_')
            metric_name = METRIC_ALIASES.get(metric_name, metric_name)

            buf[:] = []
            continue

        if char.isdigit() and not has_metric_value and has_metric_name:
            has_metric_value = True
        elif has_metric_value and char == ' ' and prev_char != '/':
            has_metric_value = False
            has_metric_name = False
            value = ''.join(buf).strip()
            if '/' in value:
                value = int(value[:value.index('/')]) / float(value[value.index('/')+1:])
            else:
                value = int(value, 10)
            metrics[metric_name] = value
            buf[:] = []
            continue
        buf.append(char)
        prev_char = char
    return metrics


def parse_queue(stdout):
    if isinstance(stdout, bytes):
        stdout = stdout.decode('utf8')
    queues = {}
    current_queue = None
    for line in stdout.splitlines(True):
        if not line:
            continue
        line = line.strip()
        if line.startswith('queue '):
            queue_name = line[len('queue '):]
            no_name = True
            level = 0
            buf = []
            for char in queue_name:
                if char == ' ':
                    if no_name:
                        level += 1
                        continue
                    break
                if char != ' ':
                    no_name = False
                    buf.append(char)
            queue_name = ''.join(buf).replace(' ', '_')
            queue = {
                'name': queue_name,
                'children': [],
                'metrics': {}
            }
            if line.endswith('}'):
                start = line[line.rindex('{')+1:-1].split(', ')
                queue['children'] = tuple(start)
            queues[queue_name] = queue
            current_queue = queue_name
        if line.startswith('[ '):
            queues[current_queue]['metrics'].update(parse_metric(line))
    return {
        queue_name: QueueMetrics(**queue) for queue_name, queue in queues.items()
    }


def summarize_children(queues: dict) -> dict:
    '''
    All parent queues have zeroed counters.

    TODO: This really should flatten from bottom -> root node or else bad things happen
    '''
    for queue_name, parent_data in ((queue, data) for queue, data in queues.items()
                                    if data.children):
        children = queues[queue_name].children
        for child_queue_data in (queues[queue_name] for queue_name in children):
            for metric_name, value in child_queue_data.items():
                parent_data[metric_name] += value
        for key, value in parent_data.items():
            if key == 'queue_load_factor':
                parent_data[key] = value / len(children)
    return queues


async def read_queue_status():
    fh = await asyncio.create_subprocess_exec(
        *shlex.split(READ_QUEUE_STATUS), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = await fh.communicate()
    logger.debug('got {} bytes from pfctl -s queue -v'.format(len(stdout)))
    if stderr:
        stderr = stderr.decode('utf8').strip()
        logger.error(f'pfctl error: {stderr}')
    if fh.returncode:
        raise AbnormalExit(fh.returncode, stderr)
    await fh.wait()
    queues = summarize_children(parse_queue(stdout))
    return queues

async def stream_queue_status():
    while True:
        logger.debug('Reading queue status')
        queues = await read_queue_status()
        queues = {key: value for key, value in queues.items() if not value['children']}
        yield queues
