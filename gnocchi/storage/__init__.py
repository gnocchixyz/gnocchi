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
import collections
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

    def __init__(self, metric, method):
        self.metric = metric
        self.method = method
        super(AggregationDoesNotExist, self).__init__(
            "Aggregation method '%s' for metric %s does not exist" %
            (method, metric))


class GranularityDoesNotExist(StorageError):
    """Error raised when the granularity doesn't exist for a metric."""

    def __init__(self, metric, granularity):
        self.metric = metric
        self.granularity = granularity
        super(GranularityDoesNotExist, self).__init__(
            "Granularity '%s' for metric %s does not exist" %
            (utils.timespan_total_seconds(granularity), metric))


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

    @staticmethod
    def _get_measures(metric, timestamp_key, aggregation, version=3):
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

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None, resample=None):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param granularity: The granularity to retrieve.
        :param resample: The granularity to resample to.
        """
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise AggregationDoesNotExist(metric, aggregation)

        if granularity is None:
            agg_timeseries = utils.parallel_map(
                self._get_measures_timeserie,
                ((metric, aggregation, ap.granularity,
                  from_timestamp, to_timestamp)
                 for ap in reversed(metric.archive_policy.definition)))
        else:
            agg_timeseries = [self._get_measures_timeserie(
                metric, aggregation, granularity,
                from_timestamp, to_timestamp)]

        if resample and granularity:
            agg_timeseries = list(map(lambda agg: agg.resample(resample),
                                      agg_timeseries))

        return list(itertools.chain(*[ts.fetch(from_timestamp, to_timestamp)
                                      for ts in agg_timeseries]))

    def _get_measures_and_unserialize(self, metric, key, aggregation):
        data = self._get_measures(metric, key, aggregation)
        try:
            return carbonara.AggregatedTimeSerie.unserialize(
                data, key, aggregation)
        except carbonara.InvalidData:
            LOG.error("Data corruption detected for %s "
                      "aggregated `%s' timeserie, granularity `%s' "
                      "around time `%s', ignoring.",
                      metric.id, aggregation, key.sampling, key)

    def _get_measures_timeserie(self, metric,
                                aggregation, granularity,
                                from_timestamp=None, to_timestamp=None):

        # Find the number of point
        for d in metric.archive_policy.definition:
            if d.granularity == granularity:
                points = d.points
                break
        else:
            raise GranularityDoesNotExist(metric, granularity)

        all_keys = None
        try:
            all_keys = self._list_split_keys_for_metric(
                metric, aggregation, granularity)
        except MetricDoesNotExist:
            for d in metric.archive_policy.definition:
                if d.granularity == granularity:
                    return carbonara.AggregatedTimeSerie(
                        sampling=granularity,
                        aggregation_method=aggregation,
                        max_size=d.points)
            raise GranularityDoesNotExist(metric, granularity)

        if from_timestamp:
            from_timestamp = carbonara.SplitKey.from_timestamp_and_sampling(
                from_timestamp, granularity)

        if to_timestamp:
            to_timestamp = carbonara.SplitKey.from_timestamp_and_sampling(
                to_timestamp, granularity)

        timeseries = list(filter(
            lambda x: x is not None,
            utils.parallel_map(
                self._get_measures_and_unserialize,
                ((metric, key, aggregation)
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
                               aggregation, oldest_mutable_timestamp):
        # NOTE(jd) We write the full split only if the driver works that way
        # (self.WRITE_FULL) or if the oldest_mutable_timestamp is out of range.
        write_full = self.WRITE_FULL or next(key) <= oldest_mutable_timestamp
        if write_full:
            try:
                existing = self._get_measures_and_unserialize(
                    metric, key, aggregation)
            except AggregationDoesNotExist:
                pass
            else:
                if existing is not None:
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

        offset, data = split.serialize(key, compressed=write_full)

        return self._store_metric_measures(metric, key, aggregation,
                                           data, offset=offset)

    def _add_measures(self, aggregation, archive_policy_def,
                      metric, grouped_serie,
                      previous_oldest_mutable_timestamp,
                      oldest_mutable_timestamp):

        if aggregation.startswith("rate:"):
            grouped_serie = grouped_serie.derived()
            aggregation_to_compute = aggregation[5:]
        else:
            aggregation_to_compute = aggregation

        ts = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped_serie, archive_policy_def.granularity,
            aggregation_to_compute, max_size=archive_policy_def.points)

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
            oldest_point_to_keep = ts.last - archive_policy_def.timespan
            oldest_key_to_keep = ts.get_split_key(oldest_point_to_keep)
            for key in list(existing_keys):
                # NOTE(jd) Only delete if the key is strictly inferior to
                # the timestamp; we don't delete any timeserie split that
                # contains our timestamp, so we prefer to keep a bit more
                # than deleting too much
                if key < oldest_key_to_keep:
                    self._delete_metric_measures(metric, key, aggregation)
                    existing_keys.remove(key)
        else:
            oldest_key_to_keep = None

        # Rewrite all read-only splits just for fun (and compression). This
        # only happens if `previous_oldest_mutable_timestamp' exists, which
        # means we already wrote some splits at some point – so this is not the
        # first time we treat this timeserie.
        if need_rewrite:
            previous_oldest_mutable_key = ts.get_split_key(
                previous_oldest_mutable_timestamp)
            oldest_mutable_key = ts.get_split_key(oldest_mutable_timestamp)

            if previous_oldest_mutable_key != oldest_mutable_key:
                for key in existing_keys:
                    if previous_oldest_mutable_key <= key < oldest_mutable_key:
                        LOG.debug(
                            "Compressing previous split %s (%s) for metric %s",
                            key, aggregation, metric)
                        # NOTE(jd) Rewrite it entirely for fun (and later for
                        # compression). For that, we just pass None as split.
                        self._store_timeserie_split(
                            metric, key,
                            None, aggregation, oldest_mutable_timestamp)

        for key, split in ts.split():
            if oldest_key_to_keep is None or key >= oldest_key_to_keep:
                LOG.debug(
                    "Storing split %s (%s) for metric %s",
                    key, aggregation, metric)
                self._store_timeserie_split(
                    metric, key, split, aggregation, oldest_mutable_timestamp)

    @staticmethod
    def _delete_metric(metric):
        raise NotImplementedError

    def delete_metric(self, incoming, metric, sync=False):
        LOG.debug("Deleting metric %s", metric)
        lock = incoming.get_sack_lock(
            self.coord, incoming.sack_for_metric(metric.id))
        if not lock.acquire(blocking=sync):
            raise LockedMetric(metric)
        # NOTE(gordc): no need to hold lock because the metric has been already
        #              marked as "deleted" in the indexer so no measure worker
        #              is going to process it anymore.
        lock.release()
        self._delete_metric(metric)
        incoming.delete_unprocessed_measures_for_metric_id(metric.id)
        LOG.debug("Deleted metric %s", metric)

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

        metrics_to_expunge = index.list_metrics(status='delete')
        for m in metrics_to_expunge:
            try:
                self.delete_metric(incoming, m, sync)
                index.expunge_metric(m.id)
            except (indexer.NoSuchMetric, LockedMetric):
                # It's possible another process deleted or is deleting the
                # metric, not a big deal
                pass
            except Exception:
                if sync:
                    raise
                LOG.error("Unable to expunge metric %s from storage", m,
                          exc_info=True)

    def process_new_measures(self, indexer, incoming, metrics_to_process,
                             sync=False):
        """Process added measures in background.

        Some drivers might need to have a background task running that process
        the measures sent to metrics. This is used for that.
        """
        # process only active metrics. deleted metrics with unprocessed
        # measures will be skipped until cleaned by janitor.
        metrics = indexer.list_metrics(ids=metrics_to_process)
        for metric in metrics:
            # NOTE(gordc): must lock at sack level
            try:
                LOG.debug("Processing measures for %s", metric)
                with incoming.process_measure_for_metric(metric) \
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

        measures.sort(order='timestamps')

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

    def _find_measure(self, metric, aggregation, granularity, predicate,
                      from_timestamp, to_timestamp):
        timeserie = self._get_measures_timeserie(
            metric, aggregation, granularity,
            from_timestamp, to_timestamp)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return {metric:
                [(timestamp, g, value)
                 for timestamp, g, value in values
                 if predicate(value)]}

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean',
                     granularity=None):
        """Search for an aggregated value that realizes a predicate.

        :param metrics: The list of metrics to look into.
        :param query: The query being sent.
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param granularity: The granularity to retrieve.
        """

        granularity = granularity or []
        predicate = MeasureQuery(query)

        results = utils.parallel_map(
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
