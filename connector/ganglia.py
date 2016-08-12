from urllib import quote_plus
from urllib import urlopen
import numpy as np
import json


class Ganglia:
    def __init__(self, root_url, cluster=None, host=None):
        self.url = root_url
        self.cluster = cluster
        self.host = host

    def retrieve_data(self, metric, start=None, end=None, cluster=None, host=None):
        params = {
            'r': 'seconds',
            'c': cluster if cluster is not None else self.cluster,
            'h': host if host is not None else self.host,
            'm': metric,
            'cs': None if start is None else self._parse_date(start),
            'ce': None if end is None else self._parse_date(end),
            'json': '1'
        }
        items = [key + '=' + quote_plus(value) for (key, value) in params.items() if value is not None]
        url = self.url + '/graph.php?' + '&'.join(items)
        http_response = urlopen(url)
        json_response = http_response.read().decode('utf-8')
        if json_response == 'null':
            return np.matrix([])
        matrix = np.matrix(json.loads(json_response)[0]['datapoints'])
        new_matrix = np.hstack([matrix[:, 1], matrix[:, 0]])
        new_matrix[:, 0] -= new_matrix[:, 0].min()
        return new_matrix

    @staticmethod
    def _parse_date(date):
        return "{:%m/%d/%Y %H:%M}".format(date)
