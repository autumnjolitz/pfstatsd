import os
import asyncio

try:
    import uvloop
except ImportError:
    uvloop = None
else:
    if os.environ.get('NO_UVLOOP', '').lower().startswith(('1', 'y', 'true')):
        uvloop = None
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


from .about import __version__
__version__  # Silence unused import warning.

DEFAULT_STDOUT_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
DEFAULT_SYSLOG_FORMAT = '%(name)s: [%(asctime)s] [%(levelname)s] %(message)s'


def parse_host(hostname, default_port):
    port = default_port
    num_cols = hostname.count(':')
    if num_cols:
        index = hostname.rindex(':')
        if num_cols > 1:
            for i in range(len(hostname)-1, index, -1):
                char = hostname[i]
                if char == ']':
                    # our nearest end colon is inside brackets. no port here.
                    index = None
                    break
            if index:
                port = hostname[index+1:]
                if port:
                    port = int(port, 10)
            ipv6_hostname = hostname[:index]
            if (ipv6_hostname[0], ipv6_hostname[-1]) != ('[', ']'):
                raise ValueError(
                    f'An IPv6 address ({hostname!r}) must be enclosed in square brackets')
            hostname = ipv6_hostname[1:-1]
        else:
            hostname, port = hostname[:index], hostname[index+1:]
            if port:
                port = int(port, 10)
    return hostname, port
