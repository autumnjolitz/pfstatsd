Pfstatsd
----------

Gather traffic statistics from PF and ingest into graphite/grafana.

Rewrite into Python 3 using asyncio.

Goal is to establish visibility of bandwidth usage by client (using ``pfctl -s state -v``), upload bandwith QoS (``pfctl -s queue -v``).


