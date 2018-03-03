import collections
import logging
import asyncio
import subprocess
import shlex
from typing import Generator, Tuple
from . import AbnormalExit

logger = logging.getLogger(__name__)
READ_QUEUE_STATUS = 'pfctl -s queue -v'

METRIC_ALIASES = {
    'qlength': 'queue_load_factor'
}


class QueueMetrics(collections.namedtuple('QueueMetrics', ['name', 'children', 'metrics', 'parent'])):
    __slots__ = ()

    def __new__(cls, name, children, metrics, parent=None):
        return super().__new__(cls, name, children, metrics, parent)




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

def apply_parents(queues) -> Generator[Tuple[str, QueueMetrics], None, None]:
    reparented_keys = set()
    for name, queue in queues.items():
        for child_name in queue.children:
            child_queue = queues[child_name]
            assert child_queue.parent is None
            # Nodes that have been parented are unique and can be emitted immediately.
            yield child_name, child_queue._replace(parent=name)
            reparented_keys.add(child_name)
    for name in queues.keys() - reparented_keys:
        yield name, queues[name]


def summarize_children(queues: dict) -> dict:
    '''
    All parent queues have zeroed counters.

    So apply all the outer edges with metric values to the parent nodes
    '''
    averaged_nodes = set()
    parents = []
    edges = tuple(queue for name, queue in queues.items() if not queue.children)
    for node in edges:
        metrics_to_apply = node.metrics

        while node.parent:
            for metric_name, value in metrics_to_apply.items():
                queues[node.parent].metrics[metric_name] += value
            node = queues[node.parent]
    stack = list(edges)
    while stack:
        node = stack.pop(0)
        if node.parent is None:
            continue
        parent = queues[node.parent]
        if parent.name in averaged_nodes:
            continue
        parent.metrics['queue_load_factor'] /= len(parent.children)
        averaged_nodes.add(parent.name)
        stack.append(parent)

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
    queues = summarize_children(dict(apply_parents(parse_queue(stdout))))
    return queues

async def stream_queue_status():
    while True:
        logger.debug('Reading queue status')
        queues = await read_queue_status()
        queues = {key: value for key, value in queues.items() if not value.children}
        yield queues
