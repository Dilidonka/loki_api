# Simple python Grafana Loki API client

## Usage
```
from loki_api import Loki

now = datetime.now(tz=pytz.timezone('Europe/Moscow'))
some_time_ago = now - timedelta(minutes=5)
loki = Loki(limit=1000)

labels = loki.get_label_values('host', some_time_ago, now)

labels_values = loki.get_label_values('host', some_time_ago, now)

data = loki.query('{host="localhost", job="nginx"}', start=some_time_ago, end=now)

for s in loki.iterate_streams('{job="%s"}' % good_job_name, now - timedelta(minutes=30), now, lines_limit=lines_limit):
    print(s.stream, len(s.values))

```


Tested with python 3.10

## TODO:
- Use requests.Session() to persist connection <https://requests.readthedocs.io/en/master/user/advanced/>
- Intercept requests connection exceptions
