#!/usr/bin/env python
#
# Copyright (c) 2014 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Tools to measure the duration of a get and a write request, can be used like:
#
#   $ python duration_perf_test.py
#
# or to simulate multiple clients workload:
#
#   $ clients=10
#   $ parallel --progress -j $clients python duration_perf_test.py \
#       --result myresults/client{} ::: $(seq 0 $clients)
#   $ python duration_perf_analyse.py myresults
#    * get_measures:
#                  Time
#    count  1000.000000
#    mean      0.032090
#    std       0.028287
#    ...
#

import argparse
import datetime
import json
import os
import random
import time

from keystoneclient.v2_0 import client as keystone_client
import requests


def timer(func):
    def inner(self, index, *args, **kwargs):
        start = time.time()
        count = func(self, index, *args, **kwargs)
        elapsed = time.time() - start
        self._timers.setdefault(func.__name__, []).append(
            (index, elapsed, count)
        )
        print(("{name} #{index} processed "
               "{count} objects in {elapsed} sec").format(
                   name=func.__name__,
                   index=index,
                   count=count or 0,
                   elapsed=elapsed))
        return count
    return inner


class PerfTools(object):
    def __init__(self, args):
        self.args = args
        self.keystone = keystone_client.Client(
            username=args.username,
            password=args.password,
            tenant_name=args.tenant_name,
            auth_url=args.auth_url)
        self.headers = {'X-Auth-Token': self.keystone.auth_token,
                        'Content-Type': 'application/json'}
        self._metrics = []
        self._timers = {}
        self.timestamp = datetime.datetime.utcnow()

    @timer
    def write_metric(self, index):
        data = json.dumps({"archive_policy_name": self.args.archive_policy})
        resp = requests.post(self.args.gnocchi_url + "/v1/metric",
                             data=data, headers=self.headers)
        try:
            self._metrics.append(json.loads(resp.content)["id"])
        except Exception:
            raise RuntimeError("Can't continue without all metrics created "
                               "(%s)" % resp.content)

    @timer
    def write_measures(self, index, metric):
        data = []
        for i in range(self.args.batch_size):
            self.timestamp += datetime.timedelta(minutes=1)
            data.append({'timestamp': self.timestamp.isoformat(),
                         'value': 100})
        resp = requests.post(
            "%s/v1/metric/%s/measures" % (self.args.gnocchi_url, metric),
            data=json.dumps(data),
            headers=self.headers)
        if resp.status_code / 100 != 2:
            print('Failed POST request to measures #%d: %s' % (index,
                                                               resp.content))
            return 0
        return self.args.batch_size

    @timer
    def get_measures(self, index, metric):
        resp = requests.get(
            "%s/v1/metric/%s/measures" % (self.args.gnocchi_url, metric),
            headers=self.headers)
        try:
            return len(json.loads(resp.content))
        except Exception:
            print('Failed GET request to measures #%d: %s' % (index,
                                                              resp.content))
            return 0

    def _get_random_metric(self):
        return self._metrics[random.randint(0, len(self._metrics) - 1)]

    def run(self):
        try:
            for index in range(self.args.metric_count):
                self.write_metric(index)

            for index in range(self.args.measure_count):
                metric = self._get_random_metric()
                self.write_measures(index, metric)
                self.get_measures(index, metric)
        finally:
            self.dump_logs()

    def dump_logs(self):
        for name, data in self._timers.items():
            filepath = "%s_%s.csv" % (self.args.result_path, name)
            dirpath = os.path.dirname(filepath)
            if dirpath and not os.path.exists(dirpath):
                os.makedirs(dirpath)
            with open(filepath, 'w') as f:
                f.write("Index,Duration,Count\n")
                for meter in data:
                    f.write("%s\n" % ",".join("%.2f" % (m if m else 0)
                                              for m in meter))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metric-count",
                        help=('Number of metrics to be created. '
                              'metrics are created one by one.'),
                        default=100,
                        type=int)
    parser.add_argument("--measure-count",
                        help='Number of measures batches to be sent.',
                        default=100,
                        type=int)
    parser.add_argument("--gnocchi-url",
                        help='Gnocchi API URL to use.',
                        default="http://localhost:8041")
    parser.add_argument("--archive-policy",
                        help='Archive policy to use.',
                        default="low")
    parser.add_argument("--os-username",
                        dest='username',
                        help='User name to use for OpenStack service access.',
                        default="admin")
    parser.add_argument("--os-tenant-name",
                        dest='tenant_name',
                        help=('Tenant name to use for '
                              'OpenStack service access.'),
                        default="admin")
    parser.add_argument("--os-password",
                        dest='password',
                        help='Password to use for OpenStack service access.',
                        default="password")
    parser.add_argument("--os-auth-url",
                        dest='auth_url',
                        help='Auth URL to use for OpenStack service access.',
                        default="http://localhost:5000/v2.0")
    parser.add_argument("--result",
                        help='path prefix to write results to.',
                        dest='result_path',
                        default="./perf_gnocchi")
    parser.add_argument("--batch-size",
                        dest='batch_size',
                        help='Number of measurements in the batch.',
                        default=100,
                        type=int)
    PerfTools(parser.parse_args()).run()

if __name__ == '__main__':
    main()
