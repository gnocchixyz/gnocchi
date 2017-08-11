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
import itertools
import operator
import struct
import uuid

from concurrent import futures
import iso8601
import msgpack
from oslo_config import cfg
from oslo_log import log
from oslo_serialization import msgpackutils
from oslo_utils import timeutils
import pandas
import six
import six.moves
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage
from gnocchi import utils


OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               default=1, min=1,
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs. Due to the Python GIL, '
                    '1 is usually faster, unless you have high latency I/O'),
    cfg.StrOpt('coordination_url',
               secret=True,
               help='Coordination driver URL'),

]

LOG = log.getLogger(__name__)


class CorruptionError(ValueError):
    """Data corrupted, damn it."""

    def __init__(self, message):
        super(CorruptionError, self).__init__(message)


class CarbonaraBasedStorage(storage.StorageDriver):
    MEASURE_PREFIX = "measure"
    UPGRADE_BATCH_SIZE = 1000

    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.aggregation_workers_number = conf.aggregation_workers_number
        if self.aggregation_workers_number == 1:
            # NOTE(jd) Avoid using futures at all if we don't want any threads.
            self._map_in_thread = self._map_no_thread
        else:
            self._map_in_thread = self._map_in_futures_threads
        self.coord, my_id = utils.get_coordinator_and_start(
            conf.coordination_url)

    def stop(self):
        self.coord.stop()

    def _lock(self, metric_id):
        lock_name = b"gnocchi-" + str(metric_id).encode('ascii')
        return self.coord.get_lock(lock_name)

    @staticmethod
    def _get_measures(metric, timestamp_key, aggregation, granularity,
                      version=3):
        raise NotImplementedError

    @staticmethod
    def _get_unaggregated_timeserie(metric, version=3):
        raise NotImplementedError

    def _get_unaggregated_timeserie_and_unserialize(
            self, metric, block_size, back_window):
        """Retrieve unaggregated timeserie for a metric and unserialize it.

        Returns a gnocchi.carbonara.BoundTimeSerie object. If the data cannot
        be retrieved, returns None.

        """
        with timeutils.StopWatch() as sw:
            raw_measures = (
                self._get_unaggregated_timeserie(
                    metric)
            )
            LOG.debug(
                "Retrieve unaggregated measures "
                "for %s in %.2fs"
                % (metric.id, sw.elapsed()))
        try:
            return carbonara.BoundTimeSerie.unserialize(
                raw_measures, block_size, back_window)
        except ValueError:
            raise CorruptionError(
                "Data corruption detected for %s "
                "unaggregated timeserie" % metric.id)

    @staticmethod
    def _store_unaggregated_timeserie(metric, data, version=3):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        raise NotImplementedError

    @staticmethod
    def _delete_unaggregated_timeserie(metric, version=3):
        raise NotImplementedError

    @staticmethod
    def _list_split_keys_for_metric(metric, aggregation, granularity,
                                    version=None):
        raise NotImplementedError

    @staticmethod
    def _version_check(name, v):
        """Validate object matches expected version.

        Version should be last attribute and start with 'v'
        """
        attrs = name.split("_")
        return not v or (not attrs[-1].startswith('v') if v == 2
                         else attrs[-1] == 'v%s' % v)

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
            return carbonara.AggregatedTimeSerie.unserialize(
                data, key, aggregation, granularity)
        except carbonara.InvalidData:
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
            for d in metric.archive_policy.definition:
                if d.granularity == granularity:
                    return carbonara.AggregatedTimeSerie(
                        sampling=granularity,
                        aggregation_method=aggregation,
                        max_size=d.points)
            raise storage.GranularityDoesNotExist(metric, granularity)

        if from_timestamp:
            from_timestamp = str(
                carbonara.SplitKey.from_timestamp_and_sampling(
                    from_timestamp, granularity))

        if to_timestamp:
            to_timestamp = str(
                carbonara.SplitKey.from_timestamp_and_sampling(
                    to_timestamp, granularity))

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

    def _store_timeserie_split(self, metric, key, split,
                               aggregation, archive_policy_def,
                               oldest_mutable_timestamp):
        # NOTE(jd) We write the full split only if the driver works that way
        # (self.WRITE_FULL) or if the oldest_mutable_timestamp is out of range.
        write_full = self.WRITE_FULL or next(key) <= oldest_mutable_timestamp
        key_as_str = str(key)
        if write_full:
            try:
                existing = self._get_measures_and_unserialize(
                    metric, key_as_str, aggregation,
                    archive_policy_def.granularity)
            except storage.AggregationDoesNotExist:
                pass
            else:
                if existing is not None:
                    if split is None:
                        split = existing
                    else:
                        split.merge(existing)

        if split is None:
            # `split' can be none if existing is None and no split was passed
            # in order to rewrite and compress the data; in that case, it means
            # the split key is present and listed, but some aggregation method
            # or granularity is missing. That means data is corrupted, but it
            # does not mean we have to fail, we can just do nothing and log a
            # warning.
            LOG.warning("No data found for metric %s, granularity %f "
                        "and aggregation method %s (split key %s): "
                        "possible data corruption",
                        metric, archive_policy_def.granularity,
                        aggregation, key)
            return

        offset, data = split.serialize(key, compressed=write_full)

        return self._store_metric_measures(
            metric, key_as_str, aggregation, archive_policy_def.granularity,
            data, offset=offset)

    def _add_measures(self, aggregation, archive_policy_def,
                      metric, grouped_serie,
                      previous_oldest_mutable_timestamp,
                      oldest_mutable_timestamp):
        ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped_serie, archive_policy_def.granularity,
            aggregation, max_size=archive_policy_def.points)

        # Don't do anything if the timeserie is empty
        if not ts:
            return

        # We only need to check for rewrite if driver is not in WRITE_FULL mode
        # and if we already stored splits once
        need_rewrite = (
            not self.WRITE_FULL
            and previous_oldest_mutable_timestamp is not None
        )

        if archive_policy_def.timespan or need_rewrite:
            existing_keys = self._list_split_keys_for_metric(
                metric, aggregation, archive_policy_def.granularity)

        # First delete old splits
        if archive_policy_def.timespan:
            oldest_point_to_keep = ts.last - datetime.timedelta(
                seconds=archive_policy_def.timespan)
            oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)
            oldest_key_to_keep_s = str(oldest_key_to_keep)
            for key in list(existing_keys):
                # NOTE(jd) Only delete if the key is strictly inferior to
                # the timestamp; we don't delete any timeserie split that
                # contains our timestamp, so we prefer to keep a bit more
                # than deleting too much
                if key < oldest_key_to_keep_s:
                    self._delete_metric_measures(
                        metric, key, aggregation,
                        archive_policy_def.granularity)
                    existing_keys.remove(key)
        else:
            oldest_key_to_keep = carbonara.SplitKey(0, 0)

        # Rewrite all read-only splits just for fun (and compression). This
        # only happens if `previous_oldest_mutable_timestamp' exists, which
        # means we already wrote some splits at some point – so this is not the
        # first time we treat this timeserie.
        if need_rewrite:
            previous_oldest_mutable_key = str(ts.get_split_key(
                previous_oldest_mutable_timestamp))
            oldest_mutable_key = str(ts.get_split_key(
                oldest_mutable_timestamp))

            if previous_oldest_mutable_key != oldest_mutable_key:
                for key in existing_keys:
                    if previous_oldest_mutable_key <= key < oldest_mutable_key:
                        # NOTE(jd) Rewrite it entirely for fun (and later for
                        # compression). For that, we just pass None as split.
                        self._store_timeserie_split(
                            metric, carbonara.SplitKey(
                                float(key), archive_policy_def.granularity),
                            None, aggregation, archive_policy_def,
                            oldest_mutable_timestamp)

        for key, split in ts.split():
            if key >= oldest_key_to_keep:
                self._store_timeserie_split(
                    metric, key, split, aggregation, archive_policy_def,
                    oldest_mutable_timestamp)

    def _encode_measures(self, measures):
        measures = list(measures)
        return struct.pack(
            "<" + self._MEASURE_SERIAL_FORMAT * len(measures),
            *list(
                itertools.chain(
                    # NOTE(jd) int(10e8) to avoid rounding errors
                    *((int(utils.datetime_to_unix(timestamp) * int(10e8)),
                       value)
                      for timestamp, value in measures))))

    def add_measures_batch(self, metrics_and_measures):
        for metric, measures in six.iteritems(metrics_and_measures):
            self._store_new_measures(metric, self._encode_measures(measures))

    @staticmethod
    def _store_new_measures(metric, data):
        raise NotImplementedError

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    def list_metric_with_measures_to_process(self, size, part, full=False):
        metrics = map(operator.itemgetter(0),
                      # Sort by the number of measures, bigger first (reverse)
                      sorted(
                          self._list_metric_with_measures_to_process(),
                          key=operator.itemgetter(1),
                          reverse=True))
        if full:
            return set(metrics)
        return set(list(metrics)[size * part:size * (part + 1)])

    @staticmethod
    def _list_metric_with_measures_to_process():
        """Return an ordered list of metrics that needs to be processed."""
        raise NotImplementedError

    @staticmethod
    def _pending_measures_to_process_count(metric_id):
        raise NotImplementedError

    def delete_metric(self, metric, sync=False):
        with self._lock(metric.id)(blocking=sync):
            # If the metric has never been upgraded, we need to delete this
            # here too
            self._delete_metric(metric)

    @staticmethod
    def _delete_metric_measures(metric, timestamp_key,
                                aggregation, granularity, version=3):
        raise NotImplementedError

    _MEASURE_SERIAL_FORMAT = "Qd"
    _MEASURE_SERIAL_LEN = struct.calcsize(_MEASURE_SERIAL_FORMAT)

    def _unserialize_measures(self, measure_id, data):
        nb_measures = len(data) // self._MEASURE_SERIAL_LEN
        try:
            measures = struct.unpack(
                "<" + self._MEASURE_SERIAL_FORMAT * nb_measures, data)
        except struct.error:
            # This either a corruption, either a v2 measures
            try:
                return msgpackutils.loads(data)
            except ValueError:
                LOG.error(
                    "Unable to decode measure %s, possible data corruption",
                    measure_id)
                raise
        return six.moves.zip(
            pandas.to_datetime(measures[::2], unit='ns'),
            itertools.islice(measures, 1, len(measures), 2))

    def measures_report(self, details=True):
        metrics, measures, full_details = self._build_report(details)
        report = {'summary': {'metrics': metrics, 'measures': measures}}
        if full_details is not None:
            report['details'] = full_details
        return report

    def _check_for_metric_upgrade(self, metric):
        lock = self._lock(metric.id)
        with lock:
            try:
                old_unaggregated = self._get_unaggregated_timeserie_and_unserialize_v2(  # noqa
                    metric)
            except (storage.MetricDoesNotExist, CorruptionError) as e:
                # NOTE(jd) This case is not really possible – you can't
                # have archives with splits and no unaggregated
                # timeserie…
                LOG.error(
                    "Unable to find unaggregated timeserie for "
                    "metric %s, unable to upgrade data: %s",
                    metric.id, e)
                return
            unaggregated = carbonara.BoundTimeSerie(
                ts=old_unaggregated.ts,
                block_size=metric.archive_policy.max_block_size,
                back_window=metric.archive_policy.back_window)
            # Upgrade unaggregated timeserie to v3
            self._store_unaggregated_timeserie(
                metric, unaggregated.serialize())
            oldest_mutable_timestamp = (
                unaggregated.first_block_timestamp()
            )
            for agg_method, d in itertools.product(
                    metric.archive_policy.aggregation_methods,
                    metric.archive_policy.definition):
                LOG.debug(
                    "Checking if the metric %s needs migration for %s"
                    % (metric, agg_method))

                try:
                    all_keys = self._list_split_keys_for_metric(
                        metric, agg_method, d.granularity, version=2)
                except storage.MetricDoesNotExist:
                    # Just try the next metric, this one has no measures
                    break
                else:
                    LOG.info("Migrating metric %s to new format" % metric)
                    timeseries = filter(
                        lambda x: x is not None,
                        self._map_in_thread(
                            self._get_measures_and_unserialize_v2,
                            ((metric, key, agg_method, d.granularity)
                             for key in all_keys))
                    )
                    ts = carbonara.AggregatedTimeSerie.from_timeseries(
                        sampling=d.granularity,
                        aggregation_method=agg_method,
                        timeseries=timeseries, max_size=d.points)
                    for key, split in ts.split():
                        self._store_timeserie_split(
                            metric, key, split,
                            ts.aggregation_method,
                            d, oldest_mutable_timestamp)
                    for key in all_keys:
                        self._delete_metric_measures(
                            metric, key, agg_method,
                            d.granularity, version=None)
            self._delete_unaggregated_timeserie(metric, version=None)
            LOG.info("Migrated metric %s to new format" % metric)

    def upgrade(self, index):
        marker = None
        while True:
            metrics = [(metric,) for metric in
                       index.list_metrics(limit=self.UPGRADE_BATCH_SIZE,
                                          marker=marker)]
            self._map_in_thread(self._check_for_metric_upgrade, metrics)
            if len(metrics) == 0:
                break
            marker = metrics[-1][0].id

    def process_new_measures(self, indexer, metrics_to_process, sync=False):
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
                LOG.debug("Cannot acquire lock for metric %s, postponing "
                          "unprocessed measures deletion" % metric_id)
        for metric in metrics:
            lock = self._lock(metric.id)
            agg_methods = list(metric.archive_policy.aggregation_methods)
            # Do not block if we cannot acquire the lock, that means some other
            # worker is doing the job. We'll just ignore this metric and may
            # get back later to it if needed.
            if lock.acquire(blocking=sync):
                try:
                    locksw = timeutils.StopWatch().start()
                    LOG.debug("Processing measures for %s" % metric)
                    with self._process_measure_for_metric(metric) as measures:
                        # NOTE(mnaser): The metric could have been handled by
                        #               another worker, ignore if no measures.
                        if len(measures) == 0:
                            LOG.debug("Skipping %s (already processed)"
                                      % metric)
                            continue

                        measures = sorted(measures, key=operator.itemgetter(0))

                        block_size = metric.archive_policy.max_block_size
                        try:
                            ts = self._get_unaggregated_timeserie_and_unserialize(  # noqa
                                metric,
                                block_size=block_size,
                                back_window=metric.archive_policy.back_window)
                        except storage.MetricDoesNotExist:
                            try:
                                self._create_metric(metric)
                            except storage.MetricAlreadyExists:
                                # Created in the mean time, do not worry
                                pass
                            ts = None
                        except CorruptionError as e:
                            LOG.error(e)
                            ts = None

                        if ts is None:
                            # This is the first time we treat measures for this
                            # metric, or data are corrupted, create a new one
                            ts = carbonara.BoundTimeSerie(
                                block_size=block_size,
                                back_window=metric.archive_policy.back_window)
                            current_first_block_timestamp = None
                        else:
                            current_first_block_timestamp = (
                                ts.first_block_timestamp()
                            )

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
                            for d in metric.archive_policy.definition:
                                ts = bound_timeserie.group_serie(
                                    d.granularity, carbonara.round_timestamp(
                                        tstamp, d.granularity * 10e8))
                                self._map_in_thread(
                                    self._add_measures,
                                    ((aggregation, d, metric, ts,
                                      current_first_block_timestamp,
                                      bound_timeserie.first_block_timestamp())
                                     for aggregation in agg_methods))

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

                            if elapsed > 0:
                                perf = " (%d points/s, %d measures/s)" % (
                                    ((number_of_operations
                                      * computed_points['number']) / elapsed),
                                    ((number_of_operations
                                      * len(measures)) / elapsed)
                                )
                            else:
                                perf = ""
                            LOG.debug(
                                "Computed new metric %s with %d new measures "
                                "in %.2f seconds%s"
                                % (metric.id, len(measures), elapsed, perf))

                        self._store_unaggregated_timeserie(metric,
                                                           ts.serialize())

                    LOG.debug("Metric %s locked during %.2f seconds" %
                              (metric.id, locksw.elapsed()))
                except Exception:
                    LOG.debug("Metric %s locked during %.2f seconds" %
                              (metric.id, locksw.elapsed()))
                    if sync:
                        raise
                    LOG.error("Error processing new measures", exc_info=True)
                finally:
                    lock.release()

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None,
                                  granularity=None,
                                  needed_overlap=100.0):
        super(CarbonaraBasedStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp,
            aggregation, reaggregation, granularity, needed_overlap)

        if reaggregation is None:
            reaggregation = aggregation

        if granularity is None:
            granularities = (
                definition.granularity
                for metric in metrics
                for definition in metric.archive_policy.definition
            )
            granularities_in_common = [
                g
                for g, occurrence in six.iteritems(
                    collections.Counter(granularities))
                if occurrence == len(metrics)
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
                        tss, reaggregation, from_timestamp, to_timestamp,
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

    @staticmethod
    def _map_no_thread(method, list_of_args):
        return list(itertools.starmap(method, list_of_args))

    def _map_in_futures_threads(self, method, list_of_args):
        with futures.ThreadPoolExecutor(
                max_workers=self.aggregation_workers_number) as executor:
            # We use 'list' to iterate all threads here to raise the first
            # exception now, not much choice
            return list(executor.map(lambda args: method(*args), list_of_args))

    @staticmethod
    def _unserialize_timeserie_v2(data):
        return carbonara.TimeSerie.from_data(
            *carbonara.TimeSerie._timestamps_and_values_from_dict(
                msgpack.loads(data, encoding='utf-8')['values']))

    def _get_unaggregated_timeserie_and_unserialize_v2(self, metric):
        """Unserialization method for unaggregated v2 timeseries."""
        data = self._get_unaggregated_timeserie(metric, version=None)
        try:
            return self._unserialize_timeserie_v2(data)
        except ValueError:
            LOG.error("Data corruption detected for %s ignoring." % metric.id)

    def _get_measures_and_unserialize_v2(self, metric, key,
                                         aggregation, granularity):
        """Unserialization method for upgrading v2 objects. Upgrade only."""
        data = self._get_measures(
            metric, key, aggregation, granularity, version=None)
        try:
            return self._unserialize_timeserie_v2(data)
        except ValueError:
            LOG.error("Data corruption detected for %s "
                      "aggregated `%s' timeserie, granularity `%s' "
                      "around time `%s', ignoring."
                      % (metric.id, aggregation, granularity, key))
