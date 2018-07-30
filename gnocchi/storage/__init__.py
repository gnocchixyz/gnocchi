# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2018 Red Hat, Inc.
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
import itertools
import operator

import daiquiri
import numpy
from oslo_config import cfg
import six

from gnocchi import carbonara
from gnocchi import utils


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
]


LOG = daiquiri.getLogger(__name__)


ATTRGETTER_METHOD = operator.attrgetter("method")
ATTRGETTER_GRANULARITY = operator.attrgetter("granularity")


class StorageError(Exception):
    pass


class MetricDoesNotExist(StorageError):
    """Error raised when this metric does not exist."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricDoesNotExist, self).__init__(
            "Metric %s does not exist" % metric)

    def jsonify(self):
        return {
            "cause": "Metric does not exist",
            "detail": {
                "metric": self.metric,
            },
        }


class AggregationDoesNotExist(StorageError):
    """Error raised when the aggregation method doesn't exists for a metric."""

    def __init__(self, metric, method, granularity):
        self.metric = metric
        self.method = method
        self.granularity = granularity
        super(AggregationDoesNotExist, self).__init__(
            "Aggregation method '%s' at granularity '%s' "
            "for metric %s does not exist" %
            (method, utils.timespan_total_seconds(granularity), metric))

    def jsonify(self):
        return {
            "cause": "Aggregation does not exist",
            "detail": {
                # FIXME(jd) Pecan does not use our JSON renderer for errors
                # So we need to convert this
                "granularity": utils.timespan_total_seconds(self.granularity),
                "aggregation_method": self.method,
            },
        }


class MetricAlreadyExists(StorageError):
    """Error raised when this metric already exists."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricAlreadyExists, self).__init__(
            "Metric %s already exists" % metric)


@utils.retry_on_exception_and_log("Unable to initialize storage driver")
def get_driver(conf):
    """Return the configured driver."""
    return utils.get_driver_class('gnocchi.storage', conf.storage)(
        conf.storage)


class Statistics(collections.defaultdict):
    class StatisticsTimeContext(object):
        def __init__(self, stats, name):
            self.stats = stats
            self.name = name + " time"

        def __enter__(self):
            self.sw = utils.StopWatch()
            self.sw.start()
            return self

        def __exit__(self, type, value, traceback):
            self.stats[self.name] += self.sw.elapsed()

    def __init__(self):
        super(Statistics, self).__init__(lambda: 0)

    def time(self, name):
        return self.StatisticsTimeContext(self, name)


class StorageDriver(object):

    # NOTE(sileht): By default we use threads, but some driver can disable
    # threads by setting this to utils.sequencial_map
    MAP_METHOD = staticmethod(utils.parallel_map)

    def __init__(self, conf):
        self.statistics = Statistics()

    @staticmethod
    def upgrade():
        pass

    def _get_splits(self, metrics_aggregations_keys, version=3):
        results = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for metric, aggregation, split in self.MAP_METHOD(
                lambda m, k, a, v: (m, a, self._get_splits_unbatched(m, k, a, v)),  # noqa
                ((metric, key, aggregation, version)
                 for metric, aggregations_and_keys
                 in six.iteritems(metrics_aggregations_keys)
                 for aggregation, keys
                 in six.iteritems(aggregations_and_keys)
                 for key in keys)):
            results[metric][aggregation].append(split)
        return results

    @staticmethod
    def _get_splits_unbatched(metric, timestamp_key, aggregation, version=3):
        raise NotImplementedError

    @staticmethod
    def _get_or_create_unaggregated_timeseries_unbatched(metric, version=3):
        """Get the unaggregated timeserie of metrics.

        If the metrics does not exist, it is created.

        :param metric: A metric.
        :param version: The storage format version number.
        """
        raise NotImplementedError

    def _get_or_create_unaggregated_timeseries(self, metrics, version=3):
        """Get the unaggregated timeserie of metrics.

        If the metrics does not exist, it is created.

        :param metrics: A list of metrics.
        :param version: The storage format version number.
        """
        return dict(
            six.moves.zip(
                metrics,
                self.MAP_METHOD(
                    utils.return_none_on_failure(
                        self._get_or_create_unaggregated_timeseries_unbatched),
                    ((metric, version) for metric in metrics))))

    @staticmethod
    def _store_unaggregated_timeseries_unbatched(metric, data, version=3):
        """Store unaggregated timeseries.

        :param metric: A metric.
        :param data: The data to store.
        :param version: Storage engine data format version
        """
        raise NotImplementedError

    def _store_unaggregated_timeseries(self, metrics_and_data, version=3):
        """Store unaggregated timeseries.

        :param metrics_and_data: A list of (metric, serialized_data) tuples
        :param version: Storage engine data format version
        """
        self.MAP_METHOD(
            utils.return_none_on_failure(
                self._store_unaggregated_timeseries_unbatched),
            ((metric, data, version) for metric, data in metrics_and_data))

    @staticmethod
    def _store_metric_splits_unbatched(metric, key, aggregation, data, offset,
                                       version=3):
        """Store a metric split.

        :param metric: A metric.
        :param key: The `carbonara.SplitKey`.
        :param aggregation: The `carbonara.Aggregation`.
        :param data: The actual data to write.
        :param offset: The offset to write to.
        :param version: Storage engine format version.
        """
        raise NotImplementedError

    def _store_metric_splits(self, metrics_keys_aggregations_data_offset,
                             version=3):
        """Store metric splits.

        Store a bunch of splits for some metrics.

        :param metrics_keys_aggregations_data_offset: A dict where keys are
                                                      `storage.Metric` and
                                                      values are a list of
                                                      (key, aggregation,
                                                       data, offset) tuples.
        :param version: Storage engine format version.
        """
        self.MAP_METHOD(
            self._store_metric_splits_unbatched,
            ((metric, key, aggregation, data, offset, version)
             for metric, keys_aggregations_data_offset
             in six.iteritems(metrics_keys_aggregations_data_offset)
             for key, aggregation, data, offset
             in keys_aggregations_data_offset))

    @staticmethod
    def _list_split_keys_unbatched(self, metric, aggregations, version=3):
        """List split keys for a metric.

        :param metric: The metric to look key for.
        :param aggregations: List of Aggregations to look for.
        :param version: Storage engine format version.
        :return: A dict where keys are Aggregation objects and values are
                 a set of SplitKey objects.
        """
        raise NotImplementedError

    def _list_split_keys(self, metrics_and_aggregations, version=3):
        """List split keys for metrics.

        :param metrics_and_aggregations: Dict of
                                         {`storage.Metric`:
                                          [`carbonara.Aggregation`]}
                                         to look for.
        :param version: Storage engine format version.
        :return: A dict where keys are `storage.Metric` and values are dicts
                 where keys are `carbonara.Aggregation` objects and values are
                 a set of `carbonara.SplitKey` objects.
        """
        metrics = list(metrics_and_aggregations.keys())
        r = self.MAP_METHOD(
            self._list_split_keys_unbatched,
            ((metric, metrics_and_aggregations[metric], version)
             for metric in metrics))
        return {
            metric: results
            for metric, results in six.moves.zip(metrics, r)
        }

    @staticmethod
    def _version_check(name, v):

        """Validate object matches expected version.

        Version should be last attribute and start with 'v'
        """
        return name.split("_")[-1] == 'v%s' % v

    def get_aggregated_measures(self, metrics_and_aggregations,
                                from_timestamp=None, to_timestamp=None):
        """Get aggregated measures from a metric.

        :param metrics_and_aggregations: The metrics and aggregations to
                                         retrieve in format
                                         {metric: [aggregation, …]}.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        """
        metrics_aggs_keys = self._list_split_keys(metrics_and_aggregations)

        for metric, aggregations_keys in six.iteritems(metrics_aggs_keys):
            for aggregation, keys in six.iteritems(aggregations_keys):
                start = (
                    carbonara.SplitKey.from_timestamp_and_sampling(
                        from_timestamp, aggregation.granularity)
                ) if from_timestamp else None

                stop = (
                    carbonara.SplitKey.from_timestamp_and_sampling(
                        to_timestamp, aggregation.granularity)
                ) if to_timestamp else None

                # Replace keys with filtered version
                metrics_aggs_keys[metric][aggregation] = [
                    key for key in sorted(keys)
                    if ((not start or key >= start)
                        and (not stop or key <= stop))
                ]

        metrics_aggregations_splits = self._get_splits_and_unserialize(
            metrics_aggs_keys)

        results = collections.defaultdict(dict)
        for metric, aggregations in six.iteritems(metrics_and_aggregations):
            for aggregation in aggregations:
                ts = carbonara.AggregatedTimeSerie.from_timeseries(
                    metrics_aggregations_splits[metric][aggregation],
                    aggregation)
                # We need to truncate because:
                # - If the driver is not in WRITE_FULL mode, then it might read
                # too much data that will be deleted once the split is
                # rewritten. Just truncate so we don't return it.
                # - If the driver is in WRITE_FULL but the archive policy has
                # been resized, we might still have too much points stored,
                # which will be deleted at a later point when new points will
                # be processed. Truncate to be sure we don't return them.
                if aggregation.timespan is not None:
                    ts.truncate(aggregation.timespan)
                results[metric][aggregation] = ts.fetch(
                    from_timestamp, to_timestamp)

        return results

    def get_measures(self, metric, aggregations,
                     from_timestamp=None, to_timestamp=None,
                     resample=None):
        """Get aggregated measures from a metric.

        Deprecated. Use `get_aggregated_measures` instead.

        :param metric: The metric measured.
        :param aggregations: The aggregations to retrieve.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param resample: The granularity to resample to.
        """
        timeseries = self.get_aggregated_measures(
            {metric: aggregations}, from_timestamp, to_timestamp)[metric]

        if resample:
            for agg, ts in six.iteritems(timeseries):
                timeseries[agg] = ts.resample(resample)

        return {
            aggmethod: list(itertools.chain(
                *[[(timestamp, timeseries[agg].aggregation.granularity, value)
                   for timestamp, value
                   in timeseries[agg].fetch(from_timestamp, to_timestamp)]
                  for agg in sorted(aggs,
                                    key=ATTRGETTER_GRANULARITY,
                                    reverse=True)]))
            for aggmethod, aggs in itertools.groupby(timeseries.keys(),
                                                     ATTRGETTER_METHOD)
        }

    def _get_splits_and_unserialize(self, metrics_aggregations_keys):
        """Get splits and unserialize them

        :param metrics_aggregations_keys: A dict where keys are
                                         `storage.Metric` and values are dict
                                          of {Aggregation: [SplitKey]} to
                                          retrieve.
        :return: A dict where keys are `storage.Metric` and values are dict
                 {aggregation: [`carbonara.AggregatedTimeSerie`]}.
        """
        raw_measures = self._get_splits(metrics_aggregations_keys)
        results = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for metric, aggregations_and_raws in six.iteritems(raw_measures):
            for aggregation, raws in six.iteritems(aggregations_and_raws):
                for key, raw in six.moves.zip(
                        metrics_aggregations_keys[metric][aggregation], raws):
                    try:
                        ts = carbonara.AggregatedTimeSerie.unserialize(
                            raw, key, aggregation)
                    except carbonara.InvalidData:
                        LOG.error("Data corruption detected for %s "
                                  "aggregated `%s' timeserie, granularity "
                                  "`%s' around time `%s', ignoring.",
                                  metric.id, aggregation.method, key.sampling,
                                  key)
                        ts = carbonara.AggregatedTimeSerie(aggregation)
                    results[metric][aggregation].append(ts)
        return results

    def _update_metric_splits(self, metrics_keys_aggregations_splits):
        """Store splits of `carbonara.`AggregatedTimeSerie` for a metric.

        This reads the existing split and merge it with the new one give as
        argument, then writing it to the storage.

        :param metrics_keys_aggregations_splits: A dict where keys are
                                                 `storage.Metric` and values
                                                 are tuples of the form
                                                 ({(key, aggregation): split},
                                                  oldest_mutable_timestamp)
        """
        metrics_splits_to_store = {}
        keys_to_get = collections.defaultdict(
            lambda: collections.defaultdict(list))
        splits_to_rewrite = collections.defaultdict(
            lambda: collections.defaultdict(list))

        for metric, (keys_and_aggregations_and_splits,
                     oldest_mutable_timestamp) in six.iteritems(
                         metrics_keys_aggregations_splits):
            for (key, aggregation), split in six.iteritems(
                    keys_and_aggregations_and_splits):
                # NOTE(jd) We write the full split only if the driver works
                # that way (self.WRITE_FULL) or if the oldest_mutable_timestamp
                # is out of range.
                if self.WRITE_FULL or next(key) <= oldest_mutable_timestamp:
                    # Update the splits that were passed as argument with the
                    # data already stored in the case that we need to rewrite
                    # them fully. First, fetch all those existing splits.
                    keys_to_get[metric][aggregation].append(key)
                    splits_to_rewrite[metric][aggregation].append(split)

        existing_data = self._get_splits_and_unserialize(keys_to_get)

        for metric, (keys_and_aggregations_and_splits,
                     oldest_mutable_timestamp) in six.iteritems(
                         metrics_keys_aggregations_splits):
            for aggregation, existing_list in six.iteritems(
                    existing_data[metric]):
                for key, split, existing in six.moves.zip(
                        keys_to_get[metric][aggregation],
                        splits_to_rewrite[metric][aggregation],
                        existing_list):
                    existing.merge(split)
                    keys_and_aggregations_and_splits[
                        (key, split.aggregation)] = existing

            keys_aggregations_data_offset = []
            for (key, aggregation), split in six.iteritems(
                    keys_and_aggregations_and_splits):
                # Do not store the split if it's empty.
                if split:
                    offset, data = split.serialize(
                        key,
                        compressed=key in keys_to_get[metric][aggregation])
                    keys_aggregations_data_offset.append(
                        (key, split.aggregation, data, offset))
            metrics_splits_to_store[metric] = keys_aggregations_data_offset

        return self._store_metric_splits(metrics_splits_to_store)

    def _compute_split_operations(self, metric, aggregations_and_timeseries,
                                  previous_oldest_mutable_timestamp,
                                  oldest_mutable_timestamp):
        """Compute changes to a metric and return operations to be done.

        Based on an aggregations list and a grouped timeseries, this computes
        what needs to be deleted and stored for a metric and returns it.

        :param metric: The metric
        :param aggregations_and_timeseries: A dictionary of timeseries of the
                                            form {aggregation: timeseries}.
        :param previous_oldest_mutable_timestamp: The previous oldest storable
                                                  timestamp from the previous
                                                  backwindow.
        :param oldest_mutable_timestamp: The current oldest storable timestamp
                                         from the current backwindow.
        :return: A tuple (keys_to_delete, keys_to_store) where keys_to_delete
                 is a set of `carbonara.SplitKey` to delete and where
                 keys_to_store is a dictionary of the form {key: aggts}
                 where key is a `carbonara.SplitKey` and aggts a
                 `carbonara.AggregatedTimeSerie` to be serialized.
        """
        # We only need to check for rewrite if driver is not in WRITE_FULL mode
        # and if we already stored splits once
        need_rewrite = (
            not self.WRITE_FULL
            and previous_oldest_mutable_timestamp is not None
        )

        aggregations_needing_list_of_keys = set()

        for aggregation, ts in six.iteritems(aggregations_and_timeseries):
            # Don't do anything if the timeseries is empty
            if not ts:
                continue

            if aggregation.timespan:
                oldest_point_to_keep = ts.truncate(aggregation.timespan)
            else:
                oldest_point_to_keep = None

            if previous_oldest_mutable_timestamp and (aggregation.timespan or
                                                      need_rewrite):
                previous_oldest_mutable_key = ts.get_split_key(
                    previous_oldest_mutable_timestamp)
                oldest_mutable_key = ts.get_split_key(oldest_mutable_timestamp)

                # only cleanup if there is a new object, as there must be a new
                # object for an old object to be cleanup
                if previous_oldest_mutable_key != oldest_mutable_key:
                    aggregations_needing_list_of_keys.add(aggregation)

        all_existing_keys = self._list_split_keys(
            {metric: aggregations_needing_list_of_keys})[metric]

        # NOTE(jd) This dict uses (key, aggregation) tuples as keys because
        # using just (key) would not carry the aggregation method and therefore
        # would not be unique per aggregation!
        keys_and_split_to_store = {}
        deleted_keys = set()

        for aggregation, ts in six.iteritems(aggregations_and_timeseries):
            # Don't do anything if the timeseries is empty
            if not ts:
                continue

            oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)

            # If we listed the keys for the aggregation, that's because we need
            # to check for cleanup and/or rewrite
            if aggregation in all_existing_keys:
                # FIXME(jd) This should be sorted by the driver and asserted it
                # is in tests. It's likely backends already sort anyway.
                existing_keys = sorted(all_existing_keys[aggregation])
                # First, check for old splits to delete
                if aggregation.timespan:
                    for key in list(existing_keys):
                        # NOTE(jd) Only delete if the key is strictly
                        # inferior the timestamp; we don't delete any
                        # timeserie split that contains our timestamp, so
                        # we prefer to keep a bit more than deleting too
                        # much
                        if key >= oldest_key_to_keep:
                            break
                        deleted_keys.add((key, aggregation))
                        existing_keys.remove(key)

                # Rewrite all read-only splits just for fun (and
                # compression). This only happens if
                # `previous_oldest_mutable_timestamp' exists, which means
                # we already wrote some splits at some point – so this is
                # not the first time we treat this timeserie.
                if need_rewrite:
                    for key in existing_keys:
                        if previous_oldest_mutable_key <= key:
                            if key >= oldest_mutable_key:
                                break
                            LOG.debug(
                                "Compressing previous split %s (%s) for "
                                "metric %s", key, aggregation.method,
                                metric)
                            # NOTE(jd) Rewrite it entirely for fun (and
                            # later for compression). For that, we just
                            # pass an empty split.
                            keys_and_split_to_store[
                                (key, aggregation)] = (
                                carbonara.AggregatedTimeSerie(
                                    aggregation)
                            )

            for key, split in ts.split():
                if key >= oldest_key_to_keep:
                    LOG.debug(
                        "Storing split %s (%s) for metric %s",
                        key, aggregation.method, metric)
                    keys_and_split_to_store[(key, aggregation)] = split

        return (deleted_keys, keys_and_split_to_store)

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    @staticmethod
    def _delete_metric_splits_unbatched(metric, keys, aggregation, version=3):
        raise NotImplementedError

    def _delete_metric_splits(self, metrics_keys_aggregations, version=3):
        """Delete splits of metrics.

        :param metrics_keys_aggregations: A dict where keys are
                                         `storage.Metric` and values are lists
                                         of (key, aggregation) tuples.
        """
        self.MAP_METHOD(
            utils.return_none_on_failure(self._delete_metric_splits_unbatched),
            ((metric, key, aggregation)
             for metric, keys_and_aggregations
             in six.iteritems(metrics_keys_aggregations)
             for key, aggregation in keys_and_aggregations))

    def add_measures_to_metrics(self, metrics_and_measures):
        """Update a metric with a new measures, computing new aggregations.

        :param metrics_and_measures: A dict there keys are `storage.Metric`
                                     objects and values are timeseries array of
                                     the new measures.
        """
        with self.statistics.time("raw measures fetch"):
            raw_measures = self._get_or_create_unaggregated_timeseries(
                metrics_and_measures.keys())
        self.statistics["raw measures fetch"] += len(metrics_and_measures)
        self.statistics["processed measures"] += sum(
            map(len, metrics_and_measures.values()))

        new_boundts = []
        splits_to_delete = {}
        splits_to_update = {}

        for metric, measures in six.iteritems(metrics_and_measures):
            measures = numpy.sort(measures, order='timestamps')

            agg_methods = list(metric.archive_policy.aggregation_methods)
            block_size = metric.archive_policy.max_block_size
            back_window = metric.archive_policy.back_window
            # NOTE(sileht): We keep one more blocks to calculate rate of change
            # correctly
            if any(filter(lambda x: x.startswith("rate:"), agg_methods)):
                back_window += 1

            if raw_measures[metric] is None:
                ts = None
            else:
                try:
                    ts = carbonara.BoundTimeSerie.unserialize(
                        raw_measures[metric], block_size, back_window)
                except carbonara.InvalidData:
                    LOG.error("Data corruption detected for %s "
                              "unaggregated timeserie, creating a new one",
                              metric.id)
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

            def _map_compute_splits_operations(bound_timeserie):
                # NOTE (gordc): bound_timeserie is entire set of
                # unaggregated measures matching largest
                # granularity. the following takes only the points
                # affected by new measures for specific granularity
                tstamp = max(bound_timeserie.first, measures['timestamps'][0])
                new_first_block_timestamp = (
                    bound_timeserie.first_block_timestamp()
                )
                computed_points['number'] = len(bound_timeserie)

                aggregations = metric.archive_policy.aggregations

                grouped_timeseries = {
                    granularity: bound_timeserie.group_serie(
                        granularity,
                        carbonara.round_timestamp(tstamp, granularity))
                    for granularity, aggregations
                    # No need to sort the aggregation, they are already
                    in itertools.groupby(aggregations, ATTRGETTER_GRANULARITY)
                }

                aggregations_and_timeseries = {
                    aggregation:
                    carbonara.AggregatedTimeSerie.from_grouped_serie(
                        grouped_timeseries[aggregation.granularity],
                        aggregation)
                    for aggregation in aggregations
                }

                deleted_keys, keys_and_split_to_store = (
                    self._compute_split_operations(
                        metric, aggregations_and_timeseries,
                        current_first_block_timestamp,
                        new_first_block_timestamp)
                )

                return (new_first_block_timestamp,
                        deleted_keys,
                        keys_and_split_to_store)

            with self.statistics.time("aggregated measures compute"):
                (new_first_block_timestamp,
                 deleted_keys,
                 keys_and_splits_to_store) = ts.set_values(
                     measures,
                     before_truncate_callback=_map_compute_splits_operations,
                )

            splits_to_delete[metric] = deleted_keys
            splits_to_update[metric] = (keys_and_splits_to_store,
                                        new_first_block_timestamp)

            new_boundts.append((metric, ts.serialize()))

        with self.statistics.time("splits delete"):
            self._delete_metric_splits(splits_to_delete)
        self.statistics["splits delete"] += len(splits_to_delete)
        with self.statistics.time("splits update"):
            self._update_metric_splits(splits_to_update)
        self.statistics["splits delete"] += len(splits_to_update)
        with self.statistics.time("raw measures store"):
            self._store_unaggregated_timeseries(new_boundts)
        self.statistics["raw measures store"] += len(new_boundts)
