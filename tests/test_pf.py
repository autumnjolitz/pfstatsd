from pfstatsd.pf import parse_queue, QueueMetrics, parse_metric, summarize_children, apply_parents

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


def test_summarize():
    queues = {
            'root': QueueMetrics('root', ('group',), {
                'pkts': 0,
                'pkts_bytes': 0,
                'dropped_pkts': 0,
                'dropped_pkts_bytes': 0,
                'queue_load_factor': 0.0
            }),
            'group': QueueMetrics('group', ('child_a', 'child_b'), {
                'pkts': 0,
                'pkts_bytes': 0,
                'dropped_pkts': 0,
                'dropped_pkts_bytes': 0,
                'queue_load_factor': 0.0
            }, 'root'),
            'child_a': QueueMetrics('child_a', (), {
                'pkts': 5,
                'pkts_bytes': 5,
                'dropped_pkts': 1,
                'dropped_pkts_bytes': 10,
                'queue_load_factor': 50.0
            }, 'group'),
            'child_b': QueueMetrics('child_b', (), {
                'pkts': 5,
                'pkts_bytes': 5,
                'dropped_pkts': 10,
                'dropped_pkts_bytes': 100,
                'queue_load_factor': 25.0
            }, 'group')
        }
    assert sum(queues['root'].metrics.values()) == 0
    queues = summarize_children(queues)
    assert sum(queues['root'].metrics.values()) > 0
    assert sum(queues['group'].metrics.values()) > 0
    assert queues['group'].metrics['queue_load_factor'] == (50+25) / 2
    assert sum(value for key, value in queues['root'].metrics.items()
               if key != 'queue_load_factor') == \
        sum(value for key, value in queues['group'].metrics.items() if key != 'queue_load_factor')


def test_apply_parents():
    queues = {
            'root': QueueMetrics('root', ('group',), {}, None),
            'group': QueueMetrics('group', ('child_a', 'child_b'), {}, None),
            'child_a': QueueMetrics('child_a', (), {}, None),
            'child_b': QueueMetrics('child_b', (), {}, None)
        }
    reparented_queues = dict(apply_parents(queues))
    assert all(queue.parent for queue in reparented_queues.values() if queue.name != 'root')
    assert reparented_queues['child_a'].parent == 'group'
    assert reparented_queues['group'].parent == 'root'


def test_apply_summarize():
    queues = {
            'root': QueueMetrics('root', ('group',), {
                'pkts': 0,
                'pkts_bytes': 0,
                'dropped_pkts': 0,
                'dropped_pkts_bytes': 0,
                'queue_load_factor': 0.0
            }),
            'group': QueueMetrics('group', ('child_a', 'child_b'), {
                'pkts': 0,
                'pkts_bytes': 0,
                'dropped_pkts': 0,
                'dropped_pkts_bytes': 0,
                'queue_load_factor': 0.0
            }, None),
            'child_a': QueueMetrics('child_a', (), {
                'pkts': 5,
                'pkts_bytes': 5,
                'dropped_pkts': 1,
                'dropped_pkts_bytes': 10,
                'queue_load_factor': 50.0
            }, None),
            'child_b': QueueMetrics('child_b', (), {
                'pkts': 5,
                'pkts_bytes': 5,
                'dropped_pkts': 10,
                'dropped_pkts_bytes': 100,
                'queue_load_factor': 25.0
            }, None)
        }
    queues = summarize_children(dict(apply_parents(queues)))
    assert sum(queues['root'].metrics.values()) > 0
    assert sum(queues['group'].metrics.values()) > 0
    assert queues['group'].metrics['queue_load_factor'] == (50+25) / 2
    assert sum(value for key, value in queues['root'].metrics.items()
               if key != 'queue_load_factor') == \
        sum(value for key, value in queues['group'].metrics.items() if key != 'queue_load_factor')
