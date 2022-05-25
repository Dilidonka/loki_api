from typing import Any, List, Dict, Tuple, Union
from enum import Enum
from datetime import datetime, timedelta
import pytz
import requests
from requests.auth import HTTPBasicAuth
from pydantic import BaseSettings
from .models import LokiListResponse, LokiStream, LokiVector, LokiMatrix, LokiStreamsResponse, LokiVectorResponse, LokiMatrixResponse


class LokiQueryError(Exception):
    pass


class LokiDirecton(Enum):
    forward = 'forward'
    backward = 'backward'


class LokiConfig(BaseSettings):
    loki_url: str
    loki_http_user: str
    loki_http_password: str

    class Config:  # pyright: ignore
        env_file = '.env'


class Loki:
    def __init__(self, limit: int = 5000) -> None:
        self.config = LokiConfig()  # pyright: ignore
        if self.config.loki_url[-1] != '/':
            self.config.loki_url += '/'

        self.limit = limit
        self.direction = LokiDirecton.forward

    def _build_logql_query(self, logql_logql_query: Dict[str, Union[str, str]]) -> str:
        '''
        Convert dict to LogQL query
        from {'host': 'myhost', 'site': ['site1', 'site2'], 'logql_str_append': '|= "POST"'}
        to сроку '{host="myhost", site=~"site1|site2"} |= "POST"'
        '''
        params: List[str] = []
        logql_str_append = ''
        for key in logql_logql_query:
            if logql_logql_query[key] is None:
                continue
            if key == 'logql_str_append':
                logql_str_append = logql_logql_query[key]
            if type(logql_logql_query[key]) is list:
                if len(logql_logql_query[key]) > 0:
                    params.append(f'{key}=~"{"|".join(logql_logql_query[key])}"')
            else:
                params.append(f'{key}="{logql_logql_query[key]}"')

        return ",".join(params) + logql_str_append

    def _http_query(self, uri: str, req_params: Dict[str, Union[str, datetime]] = {}) -> Dict[Any, Any]:
        # TODO: connection error exception
        r = requests.get(
            self.config.loki_url + uri,
            params=req_params,  # type: ignore
            auth=HTTPBasicAuth(username=self.config.loki_http_user, password=self.config.loki_http_password)
        )

        if r.status_code == 200:
            json_result = r.json()
            # print(json_result)
            if json_result.get('status') == 'success':
                return json_result

        raise LokiQueryError('status_code: %s, text: %s' % (r.status_code, r.text))

    def get_label_values(self, label: str, start: datetime, end: datetime) -> List[str]:
        json_result = self._http_query(
            uri=f'label/{label}/values',
            req_params={
                'start': str(datetime.timestamp(start)),
                'end': str(datetime.timestamp(end))
            }
        )
        return LokiListResponse(**json_result).data

    def get_labels(self, start: datetime, end: datetime) -> List[str]:
        json_result = self._http_query(
            uri='labels',
            req_params={
                'start': str(datetime.timestamp(start)),
                'end': str(datetime.timestamp(end))
            }
        )
        return LokiListResponse(**json_result).data

    def _query(self, query: str, time: datetime) -> Dict[Any, Any]:
        '''
        Perform instant query

        :param query: LogQL query string
        '''
        return self._http_query(
            uri='query',
            req_params={
                'query': query,
                'time': str(int(datetime.timestamp(time))),
                'limit': str(self.limit),
                'direction': self.direction.value
            }
        )

    def get_instant_streams(self, query: str, time: datetime) -> List[LokiStream]:
        '''
        Perform log query

        :param query: LogQL string like: {job="nginx"}
        '''
        json_result = self._query(query, time)
        if json_result.get('data'):
            resultType = json_result['data'].get('resultType')
            if resultType == 'streams':
                return LokiStreamsResponse(**json_result).data.result

        raise LokiQueryError('No result found')

    def get_instant_vector(self, query: str, time: datetime = datetime.now()) -> List[LokiVector]:
        '''
        Perform metric query

        :param query: LogQL string like: sum(count_over_time({job="nginx"}[10m]))
        '''
        json_result = self._query(query, time)
        if json_result.get('data'):
            resultType = json_result['data'].get('resultType')
            if resultType == 'vector':
                return LokiVectorResponse(**json_result).data.result

        raise LokiQueryError('No result found')

    def get_lines_count(self, logql_query: str, start: datetime, end: datetime) -> int:
        '''
        Сount lines returned by LogQL log query
        '''
        duration = int((end - start).total_seconds())
        query = 'sum(count_over_time(%s[%ss]))' % (logql_query, duration)
        r = self.get_instant_vector(query, start)
        if type(r) is list and len(r) == 1:
            return r[0].value[1]

        return 0

    def _query_range(self, query: str, start: datetime, end: datetime) -> Dict[Any, Any]:
        '''query: LogQL query string'''

        start_timestamp = int(datetime.timestamp(start))
        end_timestamp = int(datetime.timestamp(end))

        return self._http_query(
            uri='query_range',
            req_params={
                'query': query,
                'start': str(start_timestamp),
                'end': str(end_timestamp),
                'limit': str(self.limit),
                'direction': self.direction.value
            }
        )

    def get_range_streams(self, query: str, start: datetime, end: datetime) -> List[LokiStream]:
        '''
        Perform LogQL log query

        :param query: LogQL string like: {job="nginx"}
        '''
        json_result = self._query_range(query, start, end)

        if json_result.get('data'):
            resultType = json_result['data'].get('resultType')
            if resultType == 'streams':
                return LokiStreamsResponse(**json_result).data.result

        raise LokiQueryError('No result found')

    def get_range_matrix(self, query: str, start: datetime, end: datetime) -> List[LokiMatrix]:
        '''
        Perform metric query

        :param query: LogQL string like: sum(count_over_time({job="nginx"}[10m]))
        '''
        json_result = self._query_range(query, start, end)

        if json_result.get('data'):
            resultType = json_result['data'].get('resultType')

            if resultType == 'matrix':
                return LokiMatrixResponse(**json_result).data.result

        raise LokiQueryError('No result found')

    def _get_streams_batch(self, logql_query: str, start: datetime, end: datetime) -> List[LokiStream]:
        return self.get_range_streams(logql_query, start, end)

    def iterate_streams(self, logql_query: str, start: datetime, end: datetime, lines_limit: int = 10000):
        '''
        generator, returns batches of log streams

        :param query: LogQL string like: 
                        sum(count_over_time({job="nginx"}[10m]))
                        {job="nginx"}
        :param lines_limit: stop generator after lines_limit recieved. will return some more lines 
        '''
        last_log_entry: Tuple[datetime, str] = (datetime.fromtimestamp(0, tz=pytz.UTC), '')
        values_count_total = 0

        while streams_batch := self._get_streams_batch(logql_query, start, end):
            cur_stream_values_count = 0

            if len(streams_batch) == 0:
                break

            for stream_entry in streams_batch:
                values = stream_entry.values
                stream_len = len(values)
                cur_stream_values_count += stream_len
                values_count_total += stream_len

                # delete last line if dupltcate
                if last_log_entry == values[0]:
                    del values[0]

                # update last received log entry
                if stream_len > 0:
                    last_cur_stream_entry = values[-1]
                    if last_cur_stream_entry[0] > last_log_entry[0]:
                        last_log_entry = last_cur_stream_entry

                # return result
                yield stream_entry

            if cur_stream_values_count < self.limit or values_count_total >= lines_limit:
                break

            start = last_log_entry[0] + timedelta(microseconds=1)
            if start >= end:
                break


    def query(self, query: str, time: Union[datetime, None] = None, start: Union[datetime, None] = None, end: Union[datetime, None] = None) -> List[Any]:
        if time:
            if start or end:
                raise ValueError('only one of "time" or (start, end) is allowed')

            json_result = self._query(query, time)
        elif start and end:
            json_result = self._query_range(query, start, end)
        else:
            return list()

        if json_result.get('data'):
            resultType = json_result['data'].get('resultType')
            if resultType == 'vector':
                return LokiVectorResponse(**json_result).data.result
            elif resultType == 'streams':
                return LokiStreamsResponse(**json_result).data.result
            elif resultType == 'matrix':
                return LokiMatrixResponse(**json_result).data.result

        raise LokiQueryError('No result found')