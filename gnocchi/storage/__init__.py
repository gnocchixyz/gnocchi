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
import functools
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


ATTRGETTER_AGG_METHOD = operator.attrgetter("aggregation_method")
ATTRGETTER_GRANULARITY = operator.attrgetter("granularity")


class StorageError(Exception):
    pass


class InvalidQuery(StorageError):
    pass


class MetricDoesNotExist(StorageError):
    """Error raised when this metric does not exist."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricDoesNotExist, self).__init__(
            "Metric %s does not exist" % metric)


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


class StorageDriver(object):

    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def upgrade():
        pass

    def _get_measures(self, metric, keys, aggregation, version=3):
        return utils.parallel_map(
            self._get_measures_unbatched,
            ((metric, key, aggregation, version)
             for key in keys))

    @staticmethod
    def _get_measures_unbatched(metric, timestamp_key, aggregation, version=3):
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
                utils.parallel_map(
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
        utils.parallel_map(
            utils.return_none_on_failure(
                self._store_unaggregated_timeseries_unbatched),
            ((metric, data, version) for metric, data in metrics_and_data))

    @staticmethod
    def _store_metric_splits(metric, keys_and_data_and_offset, aggregation,
                             version=3):
        """Store metric split.

        Store a bunch of splits for a metric.

        :param metric: The metric to store for
        :param keys_and_data_and_offset: A list of (key, data, offset) tuples
        :param aggregation: The aggregation method concerned
        :param version: Storage engine format version.
        """
        raise NotImplementedError

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=3):
        return set(map(
            functools.partial(carbonara.SplitKey, sampling=granularity),
            (numpy.array(
                list(self._list_split_keys(
                    metric, aggregation, granularity, version)),
                dtype=numpy.float) * 10e8).astype('datetime64[ns]')))

    @staticmethod
    def _list_split_keys(metric, aggregation, granularity, version=3):
        raise NotImplementedError

    @staticmethod
    def _version_check(name, v):
        """Validate object matches expected version.

        Version should be last attribute and start with 'v'
        """
        return name.split("_")[-1] == 'v%s' % v

    def get_measures(self, metric, aggregations,
                     from_timestamp=None, to_timestamp=None,
                     resample=None):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param aggregations: The aggregations to retrieve.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param resample: The granularity to resample to.
        """
        agg_timeseries = utils.parallel_map(
            self._get_measures_timeserie,
            ((metric, ag, from_timestamp, to_timestamp)
             for ag in aggregations))

        if resample:
            agg_timeseries = list(map(lambda agg: agg.resample(resample),
                                      agg_timeseries))

        return {
            aggmethod: list(itertools.chain(
                *[ts.fetch(from_timestamp, to_timestamp)
                  for ts in aggts]))
            for aggmethod, aggts in itertools.groupby(agg_timeseries,
                                                      ATTRGETTER_AGG_METHOD)
        }

    def _get_measures_and_unserialize(self, metric, keys, aggregation):
        if not keys:
            return []
        raw_measures = self._get_measures(metric, keys, aggregation)
        results = []
        for key, raw in six.moves.zip(keys, raw_measures):
            try:
                ts = carbonara.AggregatedTimeSerie.unserialize(
                    raw, key, aggregation)
            except carbonara.InvalidData:
                LOG.error("Data corruption detected for %s "
                          "aggregated `%s' timeserie, granularity `%s' "
                          "around time `%s', ignoring.",
                          metric.id, aggregation, key.sampling, key)
            else:
                results.append(ts)
        return results

    def _get_measures_timeserie(self, metric, aggregation,
                                from_timestamp=None, to_timestamp=None):
        try:
            all_keys = self._list_split_keys_for_metric(
                metric, aggregation.method, aggregation.granularity)
        except MetricDoesNotExist:
            return carbonara.AggregatedTimeSerie(
                sampling=aggregation.granularity,
                aggregation_method=aggregation.method)

        if from_timestamp:
            from_timestamp = carbonara.SplitKey.from_timestamp_and_sampling(
                from_timestamp, aggregation.granularity)

        if to_timestamp:
            to_timestamp = carbonara.SplitKey.from_timestamp_and_sampling(
                to_timestamp, aggregation.granularity)

        keys = [key for key in sorted(all_keys)
                if ((not from_timestamp or key >= from_timestamp)
                    and (not to_timestamp or key <= to_timestamp))]

        timeseries = self._get_measures_and_unserialize(
            metric, keys, aggregation.method)

        ts = carbonara.AggregatedTimeSerie.from_timeseries(
            sampling=aggregation.granularity,
            aggregation_method=aggregation.method,
            timeseries=timeseries)
        # We need to truncate because:
        # - If the driver is not in WRITE_FULL mode, then it might read too
        # much data that will be deleted once the split is rewritten. Just
        # truncate so we don't return it.
        # - If the driver is in WRITE_FULL but the archive policy has been
        # resized, we might still have too much points stored, which will be
        # deleted at a later point when new points will be procecessed.
        # Truncate to be sure we don't return them.
        if aggregation.timespan is not None:
            ts.truncate(aggregation.timespan)
        return ts

    def _store_timeserie_splits(self, metric, keys_and_splits,
                                aggregation, oldest_mutable_timestamp,
                                oldest_point_to_keep):
        keys_to_rewrite = []
        splits_to_rewrite = []
        for key, split in six.iteritems(keys_and_splits):
            # NOTE(jd) We write the full split only if the driver works that
            # way (self.WRITE_FULL) or if the oldest_mutable_timestamp is out
            # of range.
            write_full = (
                self.WRITE_FULL or next(key) <= oldest_mutable_timestamp
            )
            if write_full:
                keys_to_rewrite.append(key)
                splits_to_rewrite.append(split)

        # Update the splits that were passed as argument with the data already
        # stored in the case that we need to rewrite them fully.
        # First, fetch all those existing splits.
        try:
            existing_data = self._get_measures_and_unserialize(
                metric, keys_to_rewrite, aggregation)
        except AggregationDoesNotExist:
            pass
        else:
            for key, split, existing in six.moves.zip(
                    keys_to_rewrite, splits_to_rewrite, existing_data):
                if existing:
                    if split is not None:
                        existing.merge(split)
                    keys_and_splits[key] = existing

        key_data_offset = []
        for key, split in six.iteritems(keys_and_splits):
            if split is None:
                # `split' can be none if existing is None and no split was
                # passed in order to rewrite and compress the data; in that
                # case, it means the split key is present and listed, but some
                # aggregation method or granularity is missing. That means data
                # is corrupted, but it does not mean we have to fail, we can
                # just do nothing and log a warning.
                LOG.warning("No data found for metric %s, granularity %f "
                            "and aggregation method %s (split key %s): "
                            "possible data corruption",
                            metric, key.sampling,
                            aggregation, key)
                continue

            if oldest_point_to_keep is not None:
                split.truncate(oldest_point_to_keep)

            offset, data = split.serialize(
                key, compressed=key in keys_to_rewrite)
            key_data_offset.append((key, data, offset))

        return self._store_metric_splits(metric, key_data_offset, aggregation)

    def _add_measures(self, metric, aggregation, grouped_serie,
                      previous_oldest_mutable_timestamp,
                      oldest_mutable_timestamp):

        if aggregation.method.startswith("rate:"):
            grouped_serie = grouped_serie.derived()
            aggregation_to_compute = aggregation.method[5:]
        else:
            aggregation_to_compute = aggregation.method

        ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped_serie, aggregation.granularity, aggregation_to_compute)

        # Don't do anything if the timeserie is empty
        if not ts:
            return

        # We only need to check for rewrite if driver is not in WRITE_FULL mode
        # and if we already stored splits once
        need_rewrite = (
            not self.WRITE_FULL
            and previous_oldest_mutable_timestamp is not None
        )

        if aggregation.timespan:
            oldest_point_to_keep = ts.last - aggregation.timespan
            oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)
        else:
            oldest_point_to_keep = None
            oldest_key_to_keep = None

        keys_and_split_to_store = {}

        if previous_oldest_mutable_timestamp and (aggregation.timespan or
                                                  need_rewrite):
            previous_oldest_mutable_key = ts.get_split_key(
                previous_oldest_mutable_timestamp)
            oldest_mutable_key = ts.get_split_key(oldest_mutable_timestamp)

            # only cleanup if there is a new object, as there must be a new
            # object for an old object to be cleanup
            if previous_oldest_mutable_key != oldest_mutable_key:
                existing_keys = sorted(self._list_split_keys_for_metric(
                    metric, aggregation.method, aggregation.granularity))

                # First, check for old splits to delete
                if aggregation.timespan:
                    deleted_keys = set()
                    for key in list(existing_keys):
                        # NOTE(jd) Only delete if the key is strictly inferior
                        # the timestamp; we don't delete any timeserie split
                        # that contains our timestamp, so we prefer to keep a
                        # bit more than deleting too much
                        if key >= oldest_key_to_keep:
                            break
                        deleted_keys.add(key)
                        existing_keys.remove(key)
                    self._delete_metric_splits(
                        metric, deleted_keys, aggregation.method)

                # Rewrite all read-only splits just for fun (and compression).
                # This only happens if `previous_oldest_mutable_timestamp'
                # exists, which means we already wrote some splits at some
                # point – so this is not the first time we treat this
                # timeserie.
                if need_rewrite:
                    for key in existing_keys:
                        if previous_oldest_mutable_key <= key:
                            if key >= oldest_mutable_key:
                                break
                            LOG.debug("Compressing previous split %s (%s) for "
                                      "metric %s", key, aggregation.method,
                                      metric)
                            # NOTE(jd) Rewrite it entirely for fun (and later
                            # for compression). For that, we just pass None as
                            # split.
                            keys_and_split_to_store[key] = None

        for key, split in ts.split():
            if oldest_key_to_keep is None or key >= oldest_key_to_keep:
                LOG.debug(
                    "Storing split %s (%s) for metric %s",
                    key, aggregation.method, metric)
                keys_and_split_to_store[key] = split

        self._store_timeserie_splits(
            metric, keys_and_split_to_store, aggregation.method,
            oldest_mutable_timestamp, oldest_point_to_keep)

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    @staticmethod
    def _delete_metric_splits_unbatched(metric, keys, aggregation, version=3):
        raise NotImplementedError

    def _delete_metric_splits(self, metric, keys, aggregation, version=3):
        utils.parallel_map(
            utils.return_none_on_failure(self._delete_metric_splits_unbatched),
            ((metric, key, aggregation) for key in keys))

    def compute_and_store_timeseries(self, metric, measures):
        # NOTE(mnaser): The metric could have been handled by
        #               another worker, ignore if no measures.
        if len(measures) == 0:
            LOG.debug("Skipping %s (already processed)", metric)
            return

        measures = numpy.sort(measures, order='timestamps')

        agg_methods = list(metric.archive_policy.aggregation_methods)
        block_size = metric.archive_policy.max_block_size
        back_window = metric.archive_policy.back_window
        definition = metric.archive_policy.definition
        # NOTE(sileht): We keep one more blocks to calculate rate of change
        # correctly
        if any(filter(lambda x: x.startswith("rate:"), agg_methods)):
            back_window += 1

        with utils.StopWatch() as sw:
            raw_measures = (
                self._get_or_create_unaggregated_timeseries(
                    [metric])[metric]
            )
        LOG.debug("Retrieve unaggregated measures for %s in %.2fs",
                  metric.id, sw.elapsed())

        if raw_measures is None:
            ts = None
        else:
            try:
                ts = carbonara.BoundTimeSerie.unserialize(
                    raw_measures, block_size, back_window)
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

        def _map_add_measures(bound_timeserie):
            # NOTE (gordc): bound_timeserie is entire set of
            # unaggregated measures matching largest
            # granularity. the following takes only the points
            # affected by new measures for specific granularity
            tstamp = max(bound_timeserie.first, measures['timestamps'][0])
            new_first_block_timestamp = bound_timeserie.first_block_timestamp()
            computed_points['number'] = len(bound_timeserie)

            for granularity, aggregations in itertools.groupby(
                    # No need to sort the aggregation, they are already
                    metric.archive_policy.aggregations,
                    ATTRGETTER_GRANULARITY):
                ts = bound_timeserie.group_serie(
                    granularity, carbonara.round_timestamp(
                        tstamp, granularity))

                utils.parallel_map(
                    self._add_measures,
                    ((metric, aggregation, ts,
                        current_first_block_timestamp,
                        new_first_block_timestamp)
                        for aggregation in aggregations))

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

        self._store_unaggregated_timeseries([(metric, ts.serialize())])

    def find_measure(self, metric, predicate, granularity, aggregation="mean",
                     from_timestamp=None, to_timestamp=None):
        agg = metric.archive_policy.get_aggregation(aggregation, granularity)
        if agg is None:
            raise AggregationDoesNotExist(metric, aggregation, granularity)

        timeserie = self._get_measures_timeserie(
            metric, agg, from_timestamp, to_timestamp)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return [(timestamp, g, value)
                for timestamp, g, value in values
                if predicate(value)]


class MeasureQuery(object):
    binary_operators = {
        u"=": operator.eq,
        u"==": operator.eq,
        u"eq": operator.eq,

        u"<": operator.lt,
        u"lt": operator.lt,

        u">": operator.gt,
        u"gt": operator.gt,

        u"<=": operator.le,
        u"≤": operator.le,
        u"le": operator.le,

        u">=": operator.ge,
        u"≥": operator.ge,
        u"ge": operator.ge,

        u"!=": operator.ne,
        u"≠": operator.ne,
        u"ne": operator.ne,

        u"%": operator.mod,
        u"mod": operator.mod,

        u"+": operator.add,
        u"add": operator.add,

        u"-": operator.sub,
        u"sub": operator.sub,

        u"*": operator.mul,
        u"×": operator.mul,
        u"mul": operator.mul,

        u"/": operator.truediv,
        u"÷": operator.truediv,
        u"div": operator.truediv,

        u"**": operator.pow,
        u"^": operator.pow,
        u"pow": operator.pow,
    }

    multiple_operators = {
        u"or": any,
        u"∨": any,
        u"and": all,
        u"∧": all,
    }

    def __init__(self, tree):
        self._eval = self.build_evaluator(tree)

    def __call__(self, value):
        return self._eval(value)

    def build_evaluator(self, tree):
        try:
            operator, nodes = list(tree.items())[0]
        except Exception:
            return lambda value: tree
        try:
            op = self.multiple_operators[operator]
        except KeyError:
            try:
                op = self.binary_operators[operator]
            except KeyError:
                raise InvalidQuery("Unknown operator %s" % operator)
            return self._handle_binary_op(op, nodes)
        return self._handle_multiple_op(op, nodes)

    def _handle_multiple_op(self, op, nodes):
        elements = [self.build_evaluator(node) for node in nodes]
        return lambda value: op((e(value) for e in elements))

    def _handle_binary_op(self, op, node):
        try:
            iterator = iter(node)
        except Exception:
            return lambda value: op(value, node)
        nodes = list(iterator)
        if len(nodes) != 2:
            raise InvalidQuery(
                "Binary operator %s needs 2 arguments, %d given" %
                (op, len(nodes)))
        node0 = self.build_evaluator(node[0])
        node1 = self.build_evaluator(node[1])
        return lambda value: op(node0(value), node1(value))
