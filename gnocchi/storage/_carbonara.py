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

from concurrent import futures
import daiquiri
import iso8601
from oslo_config import cfg
import six
import six.moves

from gnocchi import carbonara
from gnocchi import storage
from gnocchi import utils


OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               default=1, min=1,
               help='Number of threads to process and store aggregates. '
                    'Set value roughly equal to number of aggregates to be '
                    'computed per metric'),
    cfg.StrOpt('coordination_url',
               secret=True,
               help='Coordination driver URL'),

]

LOG = daiquiri.getLogger(__name__)


class CorruptionError(ValueError):
    """Data corrupted, damn it."""

    def __init__(self, message):
        super(CorruptionError, self).__init__(message)


class SackLockTimeoutError(Exception):
        pass


class CarbonaraBasedStorage(storage.StorageDriver):

    def __init__(self, conf, incoming, coord=None):
        super(CarbonaraBasedStorage, self).__init__(conf, incoming)
        self.aggregation_workers_number = conf.aggregation_workers_number
        if self.aggregation_workers_number == 1:
            # NOTE(jd) Avoid using futures at all if we don't want any threads.
            self._map_in_thread = self._map_no_thread
        else:
            self._map_in_thread = self._map_in_futures_threads
        self.coord, __ = (
            (coord, None) if coord else
            utils.get_coordinator_and_start(conf.coordination_url))
        self.shared_coord = bool(coord)

    def stop(self):
        if not self.shared_coord:
            self.coord.stop()

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
        with utils.StopWatch() as sw:
            raw_measures = (
                self._get_unaggregated_timeserie(
                    metric)
            )
        if not raw_measures:
            return
        LOG.debug(
            "Retrieve unaggregated measures "
            "for %s in %.2fs",
            metric.id, sw.elapsed())
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
    def _list_split_keys_for_metric(metric, aggregation, granularity,
                                    version=3):
        raise NotImplementedError

    @staticmethod
    def _version_check(name, v):
        """Validate object matches expected version.

        Version should be last attribute and start with 'v'
        """
        return name.split("_")[-1] == 'v%s' % v

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None, resample=None):
        super(CarbonaraBasedStorage, self).get_measures(
            metric, from_timestamp, to_timestamp, aggregation)
        if granularity is None:
            agg_timeseries = self._map_in_thread(
                self._get_measures_timeserie,
                ((metric, aggregation, ap.granularity,
                  from_timestamp, to_timestamp)
                 for ap in reversed(metric.archive_policy.definition)))
        else:
            agg_timeseries = self._get_measures_timeserie(
                metric, aggregation, granularity,
                from_timestamp, to_timestamp)
            if resample:
                agg_timeseries = agg_timeseries.resample(resample)
            agg_timeseries = [agg_timeseries]

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
                      "around time `%s', ignoring.",
                      metric.id, aggregation, granularity, key)

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

        timeseries = list(filter(
            lambda x: x is not None,
            self._map_in_thread(
                self._get_measures_and_unserialize,
                ((metric, key, aggregation, granularity)
                 for key in sorted(all_keys)
                 if ((not from_timestamp or key >= from_timestamp)
                     and (not to_timestamp or key <= to_timestamp))))
        ))

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
                        LOG.debug(
                            "Compressing previous split %s (%s) for metric %s",
                            key, aggregation, metric)
                        # NOTE(jd) Rewrite it entirely for fun (and later for
                        # compression). For that, we just pass None as split.
                        self._store_timeserie_split(
                            metric, carbonara.SplitKey(
                                float(key), archive_policy_def.granularity),
                            None, aggregation, archive_policy_def,
                            oldest_mutable_timestamp)

        for key, split in ts.split():
            if key >= oldest_key_to_keep:
                LOG.debug(
                    "Storing split %s (%s) for metric %s",
                    key, aggregation, metric)
                self._store_timeserie_split(
                    metric, key, split, aggregation, archive_policy_def,
                    oldest_mutable_timestamp)

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    def delete_metric(self, metric, sync=False):
        LOG.debug("Deleting metric %s", metric)
        lock = self.incoming.get_sack_lock(
            self.coord, self.incoming.sack_for_metric(metric.id))
        if not lock.acquire(blocking=sync):
            raise storage.LockedMetric(metric)
        # NOTE(gordc): no need to hold lock because the metric has been already
        #              marked as "deleted" in the indexer so no measure worker
        #              is going to process it anymore.
        lock.release()
        self._delete_metric(metric)
        self.incoming.delete_unprocessed_measures_for_metric_id(metric.id)

    @staticmethod
    def _delete_metric_measures(metric, timestamp_key,
                                aggregation, granularity, version=3):
        raise NotImplementedError

    def refresh_metric(self, indexer, metric, timeout):
        s = self.incoming.sack_for_metric(metric.id)
        lock = self.incoming.get_sack_lock(self.coord, s)
        if not lock.acquire(blocking=timeout):
            raise SackLockTimeoutError(
                'Unable to refresh metric: %s. Metric is locked. '
                'Please try again.' % metric.id)
        try:
            self.process_new_measures(indexer, [six.text_type(metric.id)])
        finally:
            lock.release()

    def process_new_measures(self, indexer, metrics_to_process,
                             sync=False):
        # process only active metrics. deleted metrics with unprocessed
        # measures will be skipped until cleaned by janitor.
        metrics = indexer.list_metrics(ids=metrics_to_process)
        for metric in metrics:
            # NOTE(gordc): must lock at sack level
            try:
                LOG.debug("Processing measures for %s", metric)
                with self.incoming.process_measure_for_metric(metric) \
                        as measures:
                    self._compute_and_store_timeseries(metric, measures)
                LOG.debug("Measures for metric %s processed", metric)
            except Exception:
                if sync:
                    raise
                LOG.error("Error processing new measures", exc_info=True)

    def _compute_and_store_timeseries(self, metric, measures):
        # NOTE(mnaser): The metric could have been handled by
        #               another worker, ignore if no measures.
        if len(measures) == 0:
            LOG.debug("Skipping %s (already processed)", metric)
            return

        measures = sorted(measures, key=operator.itemgetter(0))

        agg_methods = list(metric.archive_policy.aggregation_methods)
        block_size = metric.archive_policy.max_block_size
        back_window = metric.archive_policy.back_window
        definition = metric.archive_policy.definition

        try:
            ts = self._get_unaggregated_timeserie_and_unserialize(
                metric, block_size=block_size, back_window=back_window)
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
            ts = carbonara.BoundTimeSerie(block_size=block_size,
                                          back_window=back_window)
            current_first_block_timestamp = None
        else:
            current_first_block_timestamp = ts.first_block_timestamp()

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
            new_first_block_timestamp = bound_timeserie.first_block_timestamp()
            computed_points['number'] = len(bound_timeserie)
            for d in definition:
                ts = bound_timeserie.group_serie(
                    d.granularity, carbonara.round_timestamp(
                        tstamp, d.granularity * 10e8))

                self._map_in_thread(
                    self._add_measures,
                    ((aggregation, d, metric, ts,
                        current_first_block_timestamp,
                        new_first_block_timestamp)
                        for aggregation in agg_methods))

        with utils.StopWatch() as sw:
            ts.set_values(measures,
                          before_truncate_callback=_map_add_measures)

        number_of_operations = (len(agg_methods) * len(definition))
        perf = ""
        elapsed = sw.elapsed()
        if elapsed > 0:
            perf = " (%d points/s, %d measures/s)" % (
                ((number_of_operations * computed_points['number']) /
                    elapsed),
                ((number_of_operations * len(measures)) / elapsed)
            )
        LOG.debug("Computed new metric %s with %d new measures "
                  "in %.2f seconds%s",
                  metric.id, len(measures), elapsed, perf)

        self._store_unaggregated_timeserie(metric, ts.serialize())

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None, resample=None,
                                  granularity=None, needed_overlap=100.0,
                                  fill=None):
        super(CarbonaraBasedStorage, self).get_cross_metric_measures(
            metrics, from_timestamp, to_timestamp,
            aggregation, reaggregation, resample, granularity, needed_overlap)

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

        if resample and granularity:
            tss = self._map_in_thread(self._get_measures_timeserie,
                                      [(metric, aggregation, granularity,
                                        from_timestamp, to_timestamp)
                                       for metric in metrics])
            for i, ts in enumerate(tss):
                tss[i] = ts.resample(resample)
        else:
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
                        needed_overlap, fill)]
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

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean',
                     granularity=None):
        granularity = granularity or []
        predicate = storage.MeasureQuery(query)

        results = self._map_in_thread(
            self._find_measure,
            [(metric, aggregation,
              gran, predicate,
              from_timestamp, to_timestamp)
             for metric in metrics
             for gran in granularity or
             (defin.granularity
              for defin in metric.archive_policy.definition)])
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
