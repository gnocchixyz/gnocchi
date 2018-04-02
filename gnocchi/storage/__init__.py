# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
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
from gnocchi import indexer
from gnocchi import utils


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
]


LOG = daiquiri.getLogger(__name__)


ITEMGETTER_1 = operator.itemgetter(1)


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


class LockedMetric(StorageError):
    """Error raised when this metric is already being handled by another."""

    def __init__(self, metric):
        self.metric = metric
        super(LockedMetric, self).__init__("Metric %s is locked" % metric)


class CorruptionError(ValueError, StorageError):
    """Data corrupted, damn it."""

    def __init__(self, message):
        super(CorruptionError, self).__init__(message)


class SackLockTimeoutError(StorageError):
        pass


@utils.retry_on_exception_and_log("Unable to initialize storage driver")
def get_driver(conf, coord):
    """Return the configured driver."""
    return utils.get_driver_class('gnocchi.storage', conf.storage)(
        conf.storage, coord)


class StorageDriver(object):

    def __init__(self, conf, coord):
        self.coord = coord

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
        except carbonara.InvalidData:
            raise CorruptionError(
                "Data corruption detected for %s "
                "unaggregated timeserie" % metric.id)

    @staticmethod
    def _store_unaggregated_timeserie(metric, data, version=3):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, timestamp_key, aggregation,
                               data, offset=None, version=3):
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

    def get_measures(self, metric, granularities,
                     from_timestamp=None, to_timestamp=None,
                     aggregation='mean', resample=None):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param granularities: The granularities to retrieve.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param resample: The granularity to resample to.
        """

        aggregations = []
        for g in sorted(granularities, reverse=True):
            agg = metric.archive_policy.get_aggregation(aggregation, g)
            if agg is None:
                raise AggregationDoesNotExist(metric, aggregation, g)
            aggregations.append(agg)

        agg_timeseries = utils.parallel_map(
            self._get_measures_timeserie,
            ((metric, ag, from_timestamp, to_timestamp)
             for ag in aggregations))

        if resample:
            agg_timeseries = list(map(lambda agg: agg.resample(resample),
                                      agg_timeseries))

        return list(itertools.chain(*[ts.fetch(from_timestamp, to_timestamp)
                                      for ts in agg_timeseries]))

    def _get_measures_and_unserialize(self, metric, keys, aggregation):
        if not keys:
            return []
        raw_measures = self._get_measures(metric, keys, aggregation)
        results = []
        for key, raw in six.moves.zip(keys, raw_measures):
            try:
                results.append(carbonara.AggregatedTimeSerie.unserialize(
                    raw, key, aggregation))
            except carbonara.InvalidData:
                LOG.error("Data corruption detected for %s "
                          "aggregated `%s' timeserie, granularity `%s' "
                          "around time `%s', ignoring.",
                          metric.id, aggregation, key.sampling, key)
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

    def _store_timeserie_split(self, metric, key, split,
                               aggregation, oldest_mutable_timestamp,
                               oldest_point_to_keep):
        # NOTE(jd) We write the full split only if the driver works that way
        # (self.WRITE_FULL) or if the oldest_mutable_timestamp is out of range.
        write_full = self.WRITE_FULL or next(key) <= oldest_mutable_timestamp
        if write_full:
            try:
                existing = self._get_measures_and_unserialize(
                    metric, [key], aggregation)
            except AggregationDoesNotExist:
                pass
            else:
                if existing:
                    existing = existing[0]
                    if split is not None:
                        existing.merge(split)
                    split = existing

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
                        metric, key.sampling,
                        aggregation, key)
            return

        if oldest_point_to_keep is not None:
            split.truncate(oldest_point_to_keep)

        if split:
            offset, data = split.serialize(key, compressed=write_full)
            self._store_metric_measures(metric, key, aggregation,
                                        data, offset=offset)

    def _add_measures(self, aggregation, ap_def, metric, grouped_serie,
                      previous_oldest_mutable_timestamp,
                      oldest_mutable_timestamp):

        if aggregation.startswith("rate:"):
            grouped_serie = grouped_serie.derived()
            aggregation_to_compute = aggregation[5:]
        else:
            aggregation_to_compute = aggregation

        ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped_serie, ap_def.granularity, aggregation_to_compute)

        # Don't do anything if the timeserie is empty
        if not ts:
            return

        # We only need to check for rewrite if driver is not in WRITE_FULL mode
        # and if we already stored splits once
        need_rewrite = (
            not self.WRITE_FULL
            and previous_oldest_mutable_timestamp is not None
        )

        if ap_def.timespan:
            oldest_point_to_keep = ts.last - ap_def.timespan
            oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)
        else:
            oldest_point_to_keep = None
            oldest_key_to_keep = None

        if previous_oldest_mutable_timestamp and (ap_def.timespan or
                                                  need_rewrite):
            previous_oldest_mutable_key = ts.get_split_key(
                previous_oldest_mutable_timestamp)
            oldest_mutable_key = ts.get_split_key(oldest_mutable_timestamp)

            # only cleanup if there is a new object, as there must be a new
            # object for an old object to be cleanup
            if previous_oldest_mutable_key != oldest_mutable_key:
                existing_keys = sorted(self._list_split_keys_for_metric(
                    metric, aggregation, ap_def.granularity))

                # First, check for old splits to delete
                if ap_def.timespan:
                    oldest_point_to_keep = ts.last - ap_def.timespan
                    oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)
                    for key in list(existing_keys):
                        # NOTE(jd) Only delete if the key is strictly inferior
                        # the timestamp; we don't delete any timeserie split
                        # that contains our timestamp, so we prefer to keep a
                        # bit more than deleting too much
                        if key >= oldest_key_to_keep:
                            break
                        self._delete_metric_measures(metric, key, aggregation)
                        existing_keys.remove(key)

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
                                      "metric %s", key, aggregation, metric)
                            # NOTE(jd) Rewrite it entirely for fun (and later
                            # for compression). For that, we just pass None as
                            # split.
                            self._store_timeserie_split(
                                metric, key, None, aggregation,
                                oldest_mutable_timestamp, oldest_point_to_keep)

        for key, split in ts.split():
            if oldest_key_to_keep is None or key >= oldest_key_to_keep:
                LOG.debug(
                    "Storing split %s (%s) for metric %s",
                    key, aggregation, metric)
                self._store_timeserie_split(
                    metric, key, split, aggregation, oldest_mutable_timestamp,
                    oldest_point_to_keep)

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    @staticmethod
    def _delete_metric_measures(metric, timestamp_key,
                                aggregation, granularity, version=3):
        raise NotImplementedError

    def refresh_metric(self, indexer, incoming, metric, timeout):
        s = incoming.sack_for_metric(metric.id)
        lock = incoming.get_sack_lock(self.coord, s)
        if not lock.acquire(blocking=timeout):
            raise SackLockTimeoutError(
                'Unable to refresh metric: %s. Metric is locked. '
                'Please try again.' % metric.id)
        try:
            self.process_new_measures(indexer, incoming,
                                      [six.text_type(metric.id)])
        finally:
            lock.release()

    def expunge_metrics(self, incoming, index, sync=False):
        """Remove deleted metrics

        :param incoming: The incoming storage
        :param index: An indexer to be used for querying metrics
        :param sync: If True, then delete everything synchronously and raise
                     on error
        :type sync: bool
        """
        # FIXME(jd) The indexer could return them sorted/grouped by directly
        metrics_to_expunge = sorted(
            ((m, incoming.sack_for_metric(m.id))
             for m in index.list_metrics(status='delete')),
            key=ITEMGETTER_1)
        for sack, metrics in itertools.groupby(
                metrics_to_expunge, key=ITEMGETTER_1):
            try:
                lock = incoming.get_sack_lock(self.coord, sack)
                if not lock.acquire(blocking=sync):
                    # Retry later
                    LOG.debug(
                        "Sack %s is locked, cannot expunge metrics", sack)
                    continue
                # NOTE(gordc): no need to hold lock because the metric has been
                # already marked as "deleted" in the indexer so no measure
                # worker is going to process it anymore.
                lock.release()
            except Exception:
                if sync:
                    raise
                LOG.error("Unable to lock sack %s for expunging metrics",
                          sack, exc_info=True)
            else:
                for metric, sack in metrics:
                    LOG.debug("Deleting metric %s", metric)
                    try:
                        incoming.delete_unprocessed_measures_for_metric(
                            metric.id)
                        self._delete_metric(metric)
                        try:
                            index.expunge_metric(metric.id)
                        except indexer.NoSuchMetric:
                            # It's possible another process deleted or is
                            # deleting the metric, not a big deal
                            pass
                    except Exception:
                        if sync:
                            raise
                        LOG.error("Unable to expunge metric %s from storage",
                                  metric, exc_info=True)

    def process_new_measures(self, indexer, incoming, metrics_to_process,
                             sync=False):
        """Process added measures in background.

        Some drivers might need to have a background task running that process
        the measures sent to metrics. This is used for that.
        """
        # process only active metrics. deleted metrics with unprocessed
        # measures will be skipped until cleaned by janitor.
        metrics = indexer.list_metrics(
            attribute_filter={"in": {"id": metrics_to_process}})
        for metric in metrics:
            # NOTE(gordc): must lock at sack level
            try:
                LOG.debug("Processing measures for %s", metric)
                with incoming.process_measure_for_metric(metric.id) \
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

        measures = numpy.sort(measures, order='timestamps')

        agg_methods = list(metric.archive_policy.aggregation_methods)
        block_size = metric.archive_policy.max_block_size
        back_window = metric.archive_policy.back_window
        definition = metric.archive_policy.definition
        # NOTE(sileht): We keep one more blocks to calculate rate of change
        # correctly
        if any(filter(lambda x: x.startswith("rate:"), agg_methods)):
            back_window += 1

        try:
            ts = self._get_unaggregated_timeserie_and_unserialize(
                metric, block_size=block_size, back_window=back_window)
        except MetricDoesNotExist:
            try:
                self._create_metric(metric)
            except MetricAlreadyExists:
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
            tstamp = max(bound_timeserie.first, measures['timestamps'][0])
            new_first_block_timestamp = bound_timeserie.first_block_timestamp()
            computed_points['number'] = len(bound_timeserie)
            for d in definition:
                ts = bound_timeserie.group_serie(
                    d.granularity, carbonara.round_timestamp(
                        tstamp, d.granularity))

                utils.parallel_map(
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
