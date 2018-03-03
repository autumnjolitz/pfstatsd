Pfstatsd
----------

Gather traffic statistics from PF and ingest into graphite/grafana.

Rewrite into Python 3 using asyncio.

Goal is to establish visibility of bandwidth usage by client (using ``pfctl -s state -v``), upload bandwith QoS (``pfctl -s queue -v``).


Quickstart
------------

0. Spin up a docker graphite image. Or provide your own graphite.
1. Figure out how you're going to gather the data from pf. You can:
    - run ``pfstatsd`` as a user with group or other read access to /dev/pf.
    - run ``pfstatsd`` as a user that has special passwordless sudo access to just ``pfctl -s queue -v`` command
        + requires ``use_sudo: true`` in a config or ``--sudo`` argument
    - run ``pfstatsd`` as root lol

2. Run a config ``python -m pfstatsd from config/example.yml`` or ``python -m pfstatsd --sudo run -t 5 127.0.0.1:2004 yahoo.com google.com``

Issues
--------

OSX, localhost, uvloop
*************************

If you're on OSX and use localhost as your metrics destination with uvloop, you might encounter something like::

    Traceback (most recent call last):
      File "/Users/ben/software/pfstatsd/pfstatsd/__main__.py", line 21, in monitor_pf_queue
        await session.connect()
      File "/Users/ben/software/pfstatsd/pfstatsd/graphite.py", line 46, in connect
        await event_loop.sock_connect(self.conn, (self.host, self.port))
      File "uvloop/loop.pyx", line 2244, in sock_connect
      File "uvloop/loop.pyx", line 922, in uvloop.loop.Loop._sock_connect
    TypeError: getsockaddrarg() takes exactly 2 arguments (4 given)

That seems to be provoked because uvloop seems to get confused with the dual binding of localhost to ``::1`` and ``127.0.0.1`` (which is what we're trying to connect to).


No ALTQ support in kernel
****************************

You're not going to get any data from the PF part. That's for sure.


