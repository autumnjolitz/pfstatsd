from pfstatsd.pf import parse_queue, QueueMetrics, parse_metric

QUEUE_LINE = '''
queue root_ext0 on ext0 bandwidth 600Kb priority 0 {services, game_upload, web_upload, ssh_upload, bulk_upload, ack, domain_upload, torrent}
  [ pkts:          0  bytes:          0  dropped pkts:      0 bytes:      0 ]
  [ qlength:   0/ 50 ]
'''  # noqa


def test_parse_queue():
    result = parse_queue(QUEUE_LINE)
    assert result == {
        'root_ext0': QueueMetrics(**{
            'name': 'root_ext0',
            'children': ('services', 'game_upload', 'web_upload', 'ssh_upload', 'bulk_upload',
                         'ack', 'domain_upload', 'torrent'),
            'metrics': {
                'pkts': 0,
                'pkts_bytes': 0,
                'dropped_pkts': 0,
                'dropped_pkts_bytes': 0,
                'queue_load_factor': 0.0
            }
        })
    }


def test_parse_metric():
    assert parse_metric(
        '[ pkts:        822  bytes:     115520  dropped pkts:      0 bytes:      0 ]') == \
        {'pkts': 822, 'pkts_bytes': 115520, 'dropped_pkts': 0, 'dropped_pkts_bytes': 0}
    assert parse_metric('[ qlength:   1/500 ]') == {'queue_load_factor': 0.002}
