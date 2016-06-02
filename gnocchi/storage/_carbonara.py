# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
# Copyright © 2014-2015 eNovance
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
import collections
import datetime
import logging
import multiprocessing
import operator
import threading
import time
import uuid

from concurrent import futures
import iso8601
from oslo_config import cfg
from oslo_serialization import msgpackutils
from oslo_utils import timeutils
import six
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage

OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs.'),
    cfg.StrOpt('coordination_url',
               secret=True,
               help='Coordination driver URL'),

]

LOG = logging.getLogger(__name__)


class CarbonaraBasedStorage(storage.StorageDriver):
    MEASURE_PREFIX = "measure"

    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        try:
            self.coord = coordination.get_coordinator(
                conf.coordination_url,
                str(uuid.uuid4()).encode('ascii'))
            self.coord.start()
        except Exception as e:
            raise storage.StorageError("Unable to start coordinator: %s" % e)
        if conf.aggregation_workers_number is None:
            try:
                self.aggregation_workers_number = multiprocessing.cpu_count()
            except NotImplementedError:
                self.aggregation_workers_number = 2
        else:
            self.aggregation_workers_number = conf.aggregation_workers_number
        self.partition = 0
        self._stop_heartbeat = threading.Event()
        self.heartbeater = threading.Thread(target=self._heartbeat,
                                            name='heartbeat')
        self.heartbeater.setDaemon(True)
        self.heartbeater.start()

    def _heartbeat(self):
        while not self._stop_heartbeat.is_set():
            # FIXME(jd) Why 10? Why not. We should have a way to find out
            # what's the best value here, but it depends on the timeout used by
            # the driver; tooz should help us here!
            time.sleep(10)
            self.coord.heartbeat()

    def stop(self):
        self._stop_heartbeat.set()
        self.coord.stop()

    def _lock(self, metric_id):
        lock_name = b"gnocchi-" + str(metric_id).encode('ascii')
        return self.coord.get_lock(lock_name)

    @staticmethod
    def _get_measures(metric, timestamp_key, aggregation, granularity):
        raise NotImplementedError

    @staticmethod
    def _get_unaggregated_timeserie(metric):
        raise NotImplementedError

    @staticmethod
    def _store_unaggregated_timeserie(metric, data):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, granularity, data):
        raise NotImplementedError

    @staticmethod
    def _list_split_keys_for_metric(metric, aggregation, granularity):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None):
        super(CarbonaraBasedStorage, self).get_measures(
            metric, from_timestamp, to_timestamp, aggregation)
        if granularity is None:
            agg_timeseries = self._map_in_thread(
                self._get_measures_timeserie,
                ((metric, aggregation, ap.granularity,
                  from_timestamp, to_timestamp)
                 for ap in reversed(metric.archive_policy.definition)))
        else:
            agg_timeseries = [self._get_measures_timeserie(
                metric, aggregation, granularity,
                from_timestamp, to_timestamp)]
        return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                for ts in agg_timeseries
                for timestamp, r, v in ts.fetch(from_timestamp, to_timestamp)]

    def _get_measures_and_unserialize(self, metric, key,
                                      aggregation, granularity):
        data = self._get_measures(metric, key, aggregation, granularity)
        try:
            return carbonara.TimeSerie.unserialize(data)
        except ValueError:
                LOG.error("Data corruption detected for %s "
                          "aggregated `%s' timeserie, granularity `%s' "
                          "around time `%s', ignoring."
                          % (metric.id, aggregation, granularity, key))

    def _get_measures_timeserie(self, metric,
                                aggregation, granularity,
                                from_timestamp=None, to_timestamp=None):

        # Find the number of point
        for d in metric.archive_policy.definition:
            if d.granularity == granularity:
                points = d.points
                break
        else:
            raise storage.GranularityDoesNotExist(metric, granularity)

        all_keys = None
        try:
            all_keys = self._list_split_keys_for_metric(
                metric, aggregation, granularity)
        except storage.MetricDoesNotExist:
            # This can happen if it's an old metric with a TimeSerieArchive
            all_keys = None

        if not all_keys:
            # It does not mean we have no data: it can be an old metric with a
            # TimeSerieArchive.
            try:
                data = self._get_metric_archive(metric, aggregation)
            except (storage.MetricDoesNotExist,
                    storage.AggregationDoesNotExist):
                # It really does not exist
                for d in metric.archive_policy.definition:
                    if d.granularity == granularity:
                        return carbonara.AggregatedTimeSerie(
                            sampling=granularity,
                            aggregation_method=aggregation,
                            max_size=d.points)
                raise storage.GranularityDoesNotExist(metric, granularity)
            else:
                archive = carbonara.TimeSerieArchive.unserialize(data)
                # It's an old metric with an TimeSerieArchive!
                for ts in archive.agg_timeseries:
                    if ts.sampling == granularity:
                        return ts
                raise storage.GranularityDoesNotExist(metric, granularity)

        if from_timestamp:
            from_timestamp = carbonara.AggregatedTimeSerie.get_split_key(
                from_timestamp, granularity)

        if to_timestamp:
            to_timestamp = carbonara.AggregatedTimeSerie.get_split_key(
                to_timestamp, granularity)

        timeseries = filter(
            lambda x: x is not None,
            self._map_in_thread(
                self._get_measures_and_unserialize,
                ((metric, key, aggregation, granularity)
                 for key in all_keys
                 if ((not from_timestamp or key >= from_timestamp)
                     and (not to_timestamp or key <= to_timestamp))))
        )

        return carbonara.AggregatedTimeSerie.from_timeseries(
            sampling=granularity,
            aggregation_method=aggregation,
            timeseries=timeseries,
            max_size=points)

    def _add_measures(self, aggregation, archive_policy_def,
                      metric, timeserie):
        ts = self._get_measures_timeserie(metric, aggregation,
                                          archive_policy_def.granularity,
                                          timeserie.first, timeserie.last)
        ts.update(timeserie)
        for key, split in ts.split():
            self._store_metric_measures(metric, key, aggregation,
                                        archive_policy_def.granularity,
                                        split.serialize())

        if ts.last and archive_policy_def.timespan:
            oldest_point_to_keep = ts.last - datetime.timedelta(
                seconds=archive_policy_def.timespan)
            self._delete_metric_measures_before(
                metric, aggregation, archive_policy_def.granularity,
                oldest_point_to_keep)

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
    def _list_metric_with_measures_to_process(full=False):
        raise NotImplementedError

    @staticmethod
    def _pending_measures_to_process_count(metric_id):
        raise NotImplementedError

    def delete_metric(self, metric, sync=False):
        with self._lock(metric.id)(blocking=sync):
            # If the metric has never been upgraded, we need to delete this
            # here too
            self._delete_metric_archives(metric)
            self._delete_metric(metric)

    def _delete_metric_measures_before(self, metric, aggregation_method,
                                       granularity, timestamp):
        """Delete measures for a metric before a timestamp."""
        ts = carbonara.AggregatedTimeSerie.get_split_key(
            timestamp, granularity)
        for key in self._list_split_keys_for_metric(
                metric, aggregation_method, granularity):
            # NOTE(jd) Only delete if the key is strictly inferior to
            # the timestamp; we don't delete any timeserie split that
            # contains our timestamp, so we prefer to keep a bit more
            # than deleting too much
            if key < ts:
                self._delete_metric_measures(
                    metric, key, aggregation_method, granularity)

    @staticmethod
    def _delete_metric_measures(metric, timestamp_key,
                                aggregation, granularity):
        raise NotImplementedError

    @staticmethod
    def _unserialize_measures(data):
        return msgpackutils.loads(data)

    def measures_report(self, details=True):
        metrics, measures, full_details = self._build_report(details)
        report = {'summary': {'metrics': metrics, 'measures': measures}}
        if full_details is not None:
            report['details'] = full_details
        return report

    def _check_for_metric_upgrade(self, metric):
        lock = self._lock(metric.id)
        with lock:
            for agg_method in metric.archive_policy.aggregation_methods:
                LOG.debug(
                    "Checking if the metric %s needs migration for %s"
                    % (metric, agg_method))
                try:
                    data = self._get_metric_archive(metric, agg_method)
                except storage.MetricDoesNotExist:
                    # Just try the next metric, this one has no measures
                    break
                except storage.AggregationDoesNotExist:
                    # This should not happen, but you never know.
                    LOG.warning(
                        "Metric %s does not have an archive "
                        "for aggregation %s, "
                        "no migration can be done" % (metric, agg_method))
                else:
                    LOG.info("Migrating metric %s to new format" % metric)
                    archive = carbonara.TimeSerieArchive.unserialize(data)
                    for ts in archive.agg_timeseries:
                        # Store each AggregatedTimeSerie independently
                        for key, split in ts.split():
                            self._store_metric_measures(metric, key,
                                                        ts.aggregation_method,
                                                        ts.sampling,
                                                        split.serialize())
            self._delete_metric_archives(metric)
            LOG.info("Migrated metric %s to new format" % metric)

    def upgrade(self, index):
        self._map_in_thread(
            self._check_for_metric_upgrade,
            ((metric,) for metric in index.list_metrics()))

    def process_measures(self, indexer, block_size, sync=False):
        metrics_to_process = self._list_metric_with_measures_to_process(
            block_size, full=sync)
        metrics = indexer.list_metrics(ids=metrics_to_process)
        # This build the list of deleted metrics, i.e. the metrics we have
        # measures to process for but that are not in the indexer anymore.
        deleted_metrics_id = (set(map(uuid.UUID, metrics_to_process))
                              - set(m.id for m in metrics))
        for metric_id in deleted_metrics_id:
            # NOTE(jd): We need to lock the metric otherwise we might delete
            # measures that another worker might be processing. Deleting
            # measurement files under its feet is not nice!
            try:
                with self._lock(metric_id)(blocking=sync):
                    self._delete_unprocessed_measures_for_metric_id(metric_id)
            except coordination.LockAcquireFailed:
                LOG.debug("Cannot acquire lock for metric %s, postponing"
                          "unprocessed measures deletion" % metric_id)
        for metric in metrics:
            lock = self._lock(metric.id)
            agg_methods = list(metric.archive_policy.aggregation_methods)
            # Do not block if we cannot acquire the lock, that means some other
            # worker is doing the job. We'll just ignore this metric and may
            # get back later to it if needed.
            if lock.acquire(blocking=sync):
                try:
                    LOG.debug("Processing measures for %s" % metric)
                    with self._process_measure_for_metric(metric) as measures:
                        # NOTE(mnaser): The metric could have been handled by
                        #               another worker, ignore if no measures.
                        if len(measures) == 0:
                            LOG.debug("Skipping %s (already processed)"
                                      % metric)
                            continue

                        measures = sorted(measures, key=operator.itemgetter(0))
                        try:
                            with timeutils.StopWatch() as sw:
                                raw_measures = (
                                    self._get_unaggregated_timeserie(
                                        metric)
                                )
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
                        else:
                            try:
                                ts = carbonara.BoundTimeSerie.unserialize(
                                    raw_measures)
                            except ValueError:
                                ts = None
                                LOG.error(
                                    "Data corruption detected for %s "
                                    "unaggregated timeserie, "
                                    "recreating an empty one."
                                    % metric.id)

                        if ts is None:
                            # This is the first time we treat measures for this
                            # metric, or data are corrupted, create a new one
                            mbs = metric.archive_policy.max_block_size
                            ts = carbonara.BoundTimeSerie(
                                block_size=mbs,
                                back_window=metric.archive_policy.back_window)

                        # NOTE(jd) This is Python where you need such
                        # hack to pass a variable around a closure,
                        # sorry.
                        computed_points = {"number": 0}

                        def _map_add_measures(bound_timeserie):
                            # NOTE (gordc): bound_timeserie is entire set of
                            # unaggregated measures matching largest
                            # granularity. the following takes only the points
                            # affected by new measures for specific granularity
                            tstamp = max(bound_timeserie.first, measures[0][0])
                            computed_points['number'] = len(bound_timeserie)
                            self._map_in_thread(
                                self._add_measures,
                                ((aggregation, d, metric,
                                  carbonara.TimeSerie(bound_timeserie.ts[
                                      carbonara.TimeSerie.round_timestamp(
                                          tstamp, d.granularity * 10e8):]))
                                 for aggregation in agg_methods
                                 for d in metric.archive_policy.definition))

                        with timeutils.StopWatch() as sw:
                            ts.set_values(
                                measures,
                                before_truncate_callback=_map_add_measures,
                                ignore_too_old_timestamps=True)
                            elapsed = sw.elapsed()
                            number_of_operations = (
                                len(agg_methods)
                                * len(metric.archive_policy.definition)
                            )
                            speed = ((number_of_operations
                                     * computed_points['number']) / elapsed)
                            LOG.debug(
                                "Computed new metric %s with %d new measures "
                                "in %.2f seconds (%d points/s, %d measures/s)"
                                % (metric.id, len(measures), elapsed, speed,
                                   (number_of_operations * len(measures))
                                   / elapsed))

                        self._store_unaggregated_timeserie(metric,
                                                           ts.serialize())
                except Exception:
                    if sync:
                        raise
                    LOG.error("Error processing new measures", exc_info=True)
                finally:
                    lock.release()

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  granularity=None,
                                  needed_overlap=100.0):
        super(CarbonaraBasedStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp,
            aggregation, granularity, needed_overlap)

        if granularity is None:
            granularities = (
                definition.granularity
                for metric in metrics
                for definition in metric.archive_policy.definition
            )
            granularities_in_common = [
                g
                for g, occurence in six.iteritems(
                    collections.Counter(granularities))
                if occurence == len(metrics)
            ]

            if not granularities_in_common:
                raise storage.MetricUnaggregatable(
                    metrics, 'No granularity match')
        else:
            granularities_in_common = [granularity]

        tss = self._map_in_thread(self._get_measures_timeserie,
                                  [(metric, aggregation, g,
                                    from_timestamp, to_timestamp)
                                   for metric in metrics
                                   for g in granularities_in_common])
        try:
            return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                    for timestamp, r, v
                    in carbonara.AggregatedTimeSerie.aggregated(
                        tss, aggregation, from_timestamp, to_timestamp,
                        needed_overlap)]
        except carbonara.UnAggregableTimeseries as e:
            raise storage.MetricUnaggregatable(metrics, e.reason)

    def _find_measure(self, metric, aggregation, granularity, predicate,
                      from_timestamp, to_timestamp):
        timeserie = self._get_measures_timeserie(
            metric, aggregation, granularity,
            from_timestamp, to_timestamp)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return {metric:
                [(timestamp.replace(tzinfo=iso8601.iso8601.UTC),
                  g, value)
                 for timestamp, g, value in values
                 if predicate(value)]}

    # TODO(jd) Add granularity parameter here and in the REST API
    # rather than fetching all granularities
    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean'):
        predicate = storage.MeasureQuery(query)
        results = self._map_in_thread(
            self._find_measure,
            [(metric, aggregation,
              ap.granularity, predicate,
              from_timestamp, to_timestamp)
             for metric in metrics
             for ap in metric.archive_policy.definition])
        result = collections.defaultdict(list)
        for r in results:
            for metric, metric_result in six.iteritems(r):
                result[metric].extend(metric_result)

        # Sort the result
        for metric, r in six.iteritems(result):
            # Sort by timestamp asc, granularity desc
            r.sort(key=lambda t: (t[0], - t[1]))

        return result

    def _map_in_thread(self, method, list_of_args):
        with futures.ThreadPoolExecutor(
                max_workers=self.aggregation_workers_number) as executor:
            # We use 'list' to iterate all threads here to raise the first
            # exception now, not much choice
            return list(executor.map(lambda args: method(*args), list_of_args))
