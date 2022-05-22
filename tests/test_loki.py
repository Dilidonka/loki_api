from datetime import datetime, timedelta
from typing import Any, List
import pytest
from loki import Loki, LokiQueryError, LokiStream, LokiMatrix
import pytz

now = datetime.now(tz=pytz.timezone('Europe/Moscow'))
some_time_ago = now - timedelta(minutes=5)
good_job_name = 'nginx'
bad_job_name = 'nnnnn'


def test_loki_simple_queries():
    loki = Loki(limit=10)
    labels = loki.get_labels(some_time_ago, now)
    assert type(labels) is list
    assert len(labels) > 0

    labels_values = loki.get_label_values('host', some_time_ago, now)
    assert type(labels_values) is list
    assert len(labels_values) > 0

    with pytest.raises(LokiQueryError, match=r'invalid query, through < from'):
        loki.get_labels(now, some_time_ago)

    with pytest.raises(LokiQueryError, match=r'invalid query, through < from'):
        loki.get_label_values('host', now, some_time_ago)


def test_get_instant():
    loki = Loki(limit=10)
    res = loki.get_instant_streams('{job="%s"}' % good_job_name, time=now)
    assert len(res) > 0
    with pytest.raises(LokiQueryError):
        loki.get_instant_streams('{}', now)

    res = loki.get_instant_vector('sum(count_over_time({job="%s"}[300s]))' % good_job_name, now)
    assert len(res) == 1
    assert len(res) == 1
    assert type(res[0].value[1]) is int


def test_loki_query_range():
    loki = Loki(limit=10)
    res = loki.get_range_streams('{job="%s"}' % good_job_name, some_time_ago, now)
    assert len(res) > 0
    res = loki.get_range_streams('{job="%s"}' % bad_job_name, some_time_ago, now)
    assert len(res) == 0
    res = loki.get_range_matrix('count_over_time({job="%s"}[1m])' % good_job_name, some_time_ago, now)
    assert len(res) > 0
    res = loki.get_range_matrix('count_over_time({job="%s"}[1m])' % bad_job_name, some_time_ago, now)
    assert len(res) == 0

    with pytest.raises(LokiQueryError, match='end timestamp must not be before or equal to start time'):
        loki.get_range_streams('{job="%s"}' % good_job_name, now, some_time_ago)
    with pytest.raises(LokiQueryError, match='end timestamp must not be before or equal to start time'):
        loki.get_range_matrix('count_over_time({job="%s"}[1m])' % good_job_name, now, some_time_ago)


def test_get_lines_count():
    loki = Loki(limit=10)
    res = loki.get_lines_count('{job="%s"}' % good_job_name, some_time_ago, now)
    assert type(res) is int
    assert res > 0
    res = loki.get_lines_count('{job="%s"}' % bad_job_name, some_time_ago, now)
    assert type(res) is int
    assert res == 0


def test_loki_get_streams():
    loki = Loki(limit=1000)
    streams = loki._get_streams_batch('{job="%s"}' % good_job_name, now - timedelta(minutes=30), now)  # pyright: ignore
    assert len(streams) > 0
    streams = loki._get_streams_batch('{job="%s"}' % bad_job_name, now - timedelta(minutes=5), now)  # pyright: ignore
    assert len(streams) == 0

    lines_limit = 10000
    batch_size = 5000
    l = 0
    for stream in loki.iterate_streams('{job="%s"}' % good_job_name, now - timedelta(minutes=30), now, lines_limit=lines_limit):
        l += len(stream.values)
    assert l >= lines_limit < lines_limit+batch_size
