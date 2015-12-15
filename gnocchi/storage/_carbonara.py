# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
import logging
import multiprocessing
import uuid

from concurrent import futures
import iso8601
from oslo_config import cfg
from oslo_serialization import msgpackutils
from oslo_utils import timeutils
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage

OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               default=None,
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs.'),
    cfg.StrOpt('coordination_url',
               secret=True,
               help='Coordination driver URL',
               default="file:///var/lib/gnocchi/locks"),

]

LOG = logging.getLogger(__name__)


class CarbonaraBasedStorageToozLock(object):
    def __init__(self, conf):
        self.coord = coordination.get_coordinator(
            conf.coordination_url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()

    def stop(self):
        self.coord.stop()

    def __call__(self, metric):
        lock_name = b"gnocchi-" + str(metric.id).encode('ascii')
        return self.coord.get_lock(lock_name)


class CarbonaraBasedStorage(storage.StorageDriver):
    MEASURE_PREFIX = "measure"

    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.executor = futures.ThreadPoolExecutor(
            max_workers=(conf.aggregation_workers_number or
                         multiprocessing.cpu_count()))

    @staticmethod
    def _create_metric_container(metric, archive_policy):
        pass

    @staticmethod
    def _lock(metric):
        raise NotImplementedError

    @staticmethod
    def _get_measures(metric, aggregation):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, data):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None):
        super(CarbonaraBasedStorage, self).get_measures(
            metric, from_timestamp, to_timestamp, aggregation)
        archive = self._get_measures_archive(metric, aggregation)
        return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                for timestamp, r, v
                in archive.fetch(from_timestamp, to_timestamp)
                if granularity is None or r == granularity]

    @staticmethod
    def _log_data_corruption(metric, aggregation):
        LOG.error("Data are corrupted for metric %(metric)s and aggregation "
                  "%(aggregation)s, recreating an empty timeserie." %
                  dict(metric=metric.id, aggregation=aggregation))

    def _get_measures_archive(self, metric, aggregation):
        try:
            contents = self._get_measures(metric, aggregation)
        except (storage.MetricDoesNotExist, storage.AggregationDoesNotExist):
            ts = None
        else:
            try:
                ts = carbonara.TimeSerieArchive.unserialize(contents)
            except ValueError:
                self._log_data_corruption(metric, aggregation)
                ts = None

        if ts is None:
            ts = carbonara.TimeSerieArchive.from_definitions(
                [(v.granularity, v.points)
                 for v in metric.archive_policy.definition],
                aggregation_method=aggregation)
        return ts

    def _add_measures(self, aggregation, metric, timeserie):
        archive = self._get_measures_archive(metric, aggregation)
        archive.update(timeserie)
        self._store_metric_measures(metric, aggregation,
                                    archive.serialize())

    def add_measures(self, metric, measures):
        self._store_measures(metric, msgpackutils.dumps(
            list(map(tuple, measures))))

    @staticmethod
    def _store_measures(metric, data):
        raise NotImplementedError

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    @staticmethod
    def _list_metric_with_measures_to_process(metric_id):
        raise NotImplementedError

    @staticmethod
    def _pending_measures_to_process_count(metric_id):
        raise NotImplementedError

    def delete_metric(self, metric):
        with self._lock(metric):
            self._delete_metric(metric)

    @staticmethod
    def _unserialize_measures(data):
        return msgpackutils.loads(data)

    def measures_report(self):
        metrics_to_process = self._list_metric_with_measures_to_process()
        return dict(
            (metric_id, self._pending_measures_to_process_count(metric_id))
            for metric_id in metrics_to_process)

    def process_measures(self, indexer, sync=False):
        metrics_to_process = self._list_metric_with_measures_to_process()
        metrics = indexer.get_metrics(metrics_to_process)
        # This build the list of deleted metrics, i.e. the metrics we have
        # measures to process for but that are not in the indexer anymore.
        deleted_metrics_id = (set(map(uuid.UUID, metrics_to_process))
                              - set(m.id for m in metrics))
        for metric_id in deleted_metrics_id:
            self._delete_unprocessed_measures_for_metric_id(metric_id)
        for metric in metrics:
            lock = self._lock(metric)
            agg_methods = list(metric.archive_policy.aggregation_methods)
            # Do not block if we cannot acquire the lock, that means some other
            # worker is doing the job. We'll just ignore this metric and may
            # get back later to it if needed.
            if lock.acquire(blocking=sync):
                try:
                    LOG.debug("Processing measures for %s" % metric)
                    with self._process_measure_for_metric(metric) as measures:
                        try:
                            with timeutils.StopWatch() as sw:
                                raw_measures = self._get_measures(metric,
                                                                  'none')
                                LOG.debug(
                                    "Retrieve unaggregated measures "
                                    "for %s in %.2fs"
                                    % (metric.id, sw.elapsed()))
                        except storage.MetricDoesNotExist:
                            try:
                                self._create_metric(metric)
                            except storage.MetricAlreadyExists:
                                # Created in the mean time, do not worry
                                pass
                            ts = None
                        except storage.AggregationDoesNotExist:
                            ts = None
                        else:
                            try:
                                ts = carbonara.BoundTimeSerie.unserialize(
                                    raw_measures)
                            except ValueError:
                                ts = None
                                self._log_data_corruption(metric, "none")

                        if ts is None:
                            # This is the first time we treat measures for this
                            # metric, or data are corrupted,
                            # create a new one
                            mbs = metric.archive_policy.max_block_size
                            ts = carbonara.BoundTimeSerie(
                                block_size=mbs,
                                back_window=metric.archive_policy.back_window)

                        def _map_add_measures(bound_timeserie):
                            self._map_in_thread(
                                self._add_measures,
                                list((aggregation, metric, bound_timeserie)
                                     for aggregation in agg_methods))

                        with timeutils.StopWatch() as sw:
                            ts.set_values(
                                measures,
                                before_truncate_callback=_map_add_measures,
                                ignore_too_old_timestamps=True)
                            LOG.debug(
                                "Computed new metric %s with %d new measures "
                                "in %.2f seconds"
                                % (metric.id, len(measures), sw.elapsed()))

                        self._store_metric_measures(metric, 'none',
                                                    ts.serialize())
                except Exception:
                    if sync:
                        raise
                    LOG.error("Error processing new measures", exc_info=True)
                finally:
                    lock.release()

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  needed_overlap=100.0):
        super(CarbonaraBasedStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp, aggregation, needed_overlap)

        tss = self._map_in_thread(self._get_measures_archive,
                                  [(metric, aggregation)
                                   for metric in metrics])
        try:
            return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                    for timestamp, r, v
                    in carbonara.TimeSerieArchive.aggregated(
                        tss, from_timestamp, to_timestamp,
                        aggregation, needed_overlap)]
        except carbonara.UnAggregableTimeseries as e:
            raise storage.MetricUnaggregatable(metrics, e.reason)

    def _find_measure(self, metric, aggregation, predicate,
                      from_timestamp, to_timestamp):
        timeserie = self._get_measures_archive(metric, aggregation)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return {metric:
                [(timestamp.replace(tzinfo=iso8601.iso8601.UTC),
                  granularity, value)
                 for timestamp, granularity, value in values
                 if predicate(value)]}

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean'):
        result = {}
        predicate = storage.MeasureQuery(query)
        results = self._map_in_thread(self._find_measure,
                                      [(metric, aggregation, predicate,
                                        from_timestamp, to_timestamp)
                                       for metric in metrics])
        for r in results:
            result.update(r)
        return result

    def _map_in_thread(self, method, list_of_args):
        # We use 'list' to iterate all threads here to raise the first
        # exception now , not much choice
        return list(self.executor.map(lambda args: method(*args),
                                      list_of_args))
