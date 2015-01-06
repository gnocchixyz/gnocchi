# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import multiprocessing
import random
import uuid

from concurrent import futures
from oslo.config import cfg
import pandas
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage

OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               default=None,
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs.'),
]
cfg.CONF.register_opts(OPTS, group="storage")


class CarbonaraBasedStorage(storage.StorageDriver):
    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.aggregation_types = list(storage.AGGREGATION_TYPES)
        self.coord = coordination.get_coordinator(
            conf.coordination_url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()
        self.executor = futures.ThreadPoolExecutor(
            max_workers=(conf.aggregation_workers_number or
                         multiprocessing.cpu_count()))
        # NOTE(jd) So this is a (smart?) optimization: since we're going to
        # lock for each of this aggregation type, if we are using running
        # Gnocchi with multiple processes, let's randomize what we iter
        # over so there are less chances we fight for the same lock!

        random.shuffle(self.aggregation_types)

    def __del__(self):
        self.coord.stop()

    @staticmethod
    def _create_metric_container(metric):
        pass

    def create_metric(self, metric, archive_policy):
        self._create_metric_container(metric)
        for aggregation in self.aggregation_types:
            # TODO(jd) Having the TimeSerieArchive.full_res_timeserie duped in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            # TODO(jd) We should not use Pandas here
            # – abstraction layer violation!
            archive = carbonara.TimeSerieArchive.from_definitions(
                [(pandas.tseries.offsets.Second(v.granularity), v.points)
                 for v in archive_policy.definition],
                back_window=archive_policy.back_window,
                aggregation_method=aggregation)
            self._store_metric_measures(metric, aggregation,
                                        archive.serialize())

    @staticmethod
    def _get_measures(metric, aggregation):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, data):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        archive = self._get_measures_archive(metric, aggregation)
        return archive.fetch(from_timestamp, to_timestamp)

    def _get_measures_archive(self, metric, aggregation):
        contents = self._get_measures(metric, aggregation)
        return carbonara.TimeSerieArchive.unserialize(contents)

    def _add_measures(self, aggregation, metric, measures):
        lock_name = (b"gnocchi-" + metric.encode('ascii')
                     + b"-" + aggregation.encode('ascii'))
        lock = self.coord.get_lock(lock_name)
        with lock:
            contents = self._get_measures(metric, aggregation)
            archive = carbonara.TimeSerieArchive.unserialize(contents)
            try:
                archive.set_values([(m.timestamp, m.value)
                                    for m in measures])
            except carbonara.NoDeloreanAvailable as e:
                raise storage.NoDeloreanAvailable(e.first_timestamp,
                                                  e.bad_timestamp)
            self._store_metric_measures(metric, aggregation,
                                        archive.serialize())

    def add_measures(self, metric, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        self._map_in_thread(self._add_measures,
                            list((aggregation, metric, measures)
                                 for aggregation in self.aggregation_types))

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean'):

        tss = self._map_in_thread(self._get_measures_archive,
                                  [(metric, aggregation)
                                   for metric in metrics])
        try:
            return carbonara.TimeSerieArchive.aggregated(
                tss, from_timestamp, to_timestamp, aggregation)
        except carbonara.UnAggregableTimeseries as e:
            raise storage.MetricUnaggregatable(metrics, e.reason)

    def _map_in_thread(self, method, list_of_args):
        # We use 'list' to iterate all threads here to raise the first
        # exception now , not much choice
        return list(self.executor.map(lambda args: method(*args),
                                      list_of_args))
