Pfstatsd
----------

Gather traffic statistics from PF and ingest into graphite/grafana.

Rewrite into Python 3 using asyncio.

Goal is to establish visibility of bandwidth usage by client (using ``pfctl -s state -v``), upload bandwith QoS (``pfctl -s queue -v``).


Supports
----------

- Autoreconnects to Graphite ✅
- PF
    + ALTQ counters ✅
        - pkts/pkt bytes, queue length
    + Number of connection states per internal client ❌
    + Packet count to an internal client ❌
- ICMP
    + Latency ✅ 
    + Sent/Recv ✅ 
    + Lost packets ⚠️ (doesn't handle blackholes correctly yet)



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

No ALTQ support in kernel
****************************

You're not going to get any data from the PF part. That's for sure.


