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
"""Timeseries cross-aggregation."""
import collections

import daiquiri
import numpy
import six

from gnocchi import carbonara
from gnocchi.rest.aggregates import operations as agg_operations
from gnocchi import storage as gnocchi_storage


LOG = daiquiri.getLogger(__name__)


class UnAggregableTimeseries(Exception):
    """Error raised when timeseries cannot be aggregated."""
    def __init__(self, reason):
        self.reason = reason
        super(UnAggregableTimeseries, self).__init__(reason)


class MetricUnaggregatable(Exception):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics_and_aggregations, reason):
        self.metrics_and_aggregations = metrics_and_aggregations
        self.reason = reason
        metrics = ("%s/%s" % (m.id, a) for (m, a) in metrics_and_aggregations)
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s" % (
                ", ".join(metrics), reason))


def _get_measures_timeserie(storage, metric, aggregation, ref_identifier,
                            *args, **kwargs):
    return ([str(getattr(metric, ref_identifier)), aggregation],
            storage._get_measures_timeserie(metric, aggregation, *args,
                                            **kwargs))


def get_measures(storage, metrics_and_aggregations,
                 operations,
                 from_timestamp=None, to_timestamp=None,
                 granularity=None, needed_overlap=100.0,
                 fill=None, resample=None, ref_identifier="id"):
    """Get aggregated measures of multiple entities.

    :param storage: The storage driver.
    :param metrics_and_aggregations: List of metric+agg_method tuple
                                     measured to aggregate.
    :param from timestamp: The timestamp to get the measure from.
    :param to timestamp: The timestamp to get the measure to.
    :param granularity: The granularity to retrieve.
    :param fill: The value to use to fill in missing data in series.
    :param resample: The granularity to resample to.
    """
    for (metric, aggregation) in metrics_and_aggregations:
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise gnocchi_storage.AggregationDoesNotExist(metric, aggregation)
        if granularity is not None:
            for d in metric.archive_policy.definition:
                if d.granularity == granularity:
                    break
            else:
                raise gnocchi_storage.GranularityDoesNotExist(
                    metric, granularity)

    if granularity is None:
        granularities = (
            definition.granularity
            for (metric, aggregation) in metrics_and_aggregations
            for definition in metric.archive_policy.definition
        )
        granularities_in_common = [
            g
            for g, occurrence in six.iteritems(
                collections.Counter(granularities))
            if occurrence == len(metrics_and_aggregations)
        ]

        if not granularities_in_common:
            raise MetricUnaggregatable(
                metrics_and_aggregations, 'No granularity match')
    else:
        granularities_in_common = [granularity]

    tss = storage._map_in_thread(_get_measures_timeserie,
                                 [(storage, metric, aggregation,
                                   ref_identifier,
                                   g, from_timestamp, to_timestamp)
                                  for (metric, aggregation)
                                  in metrics_and_aggregations
                                  for g in granularities_in_common])

    if resample and granularity:
        tss = list(map(lambda ref_and_ts: (
            ref_and_ts[0], ref_and_ts[1].resample(resample)), tss))

    try:
        return aggregated(tss, operations, from_timestamp, to_timestamp,
                          needed_overlap, fill)
    except (UnAggregableTimeseries, carbonara.UnknownAggregationMethod) as e:
        raise MetricUnaggregatable(metrics_and_aggregations, e.reason)


def aggregated(refs_and_timeseries, operations, from_timestamp=None,
               to_timestamp=None, needed_percent_of_overlap=100.0, fill=None):

    series = collections.defaultdict(list)
    references = collections.defaultdict(list)
    for (reference, timeserie) in refs_and_timeseries:
        from_ = (None if from_timestamp is None else
                 carbonara.round_timestamp(from_timestamp, timeserie.sampling))
        references[timeserie.sampling].append(reference)
        series[timeserie.sampling].append(timeserie[from_:to_timestamp])

    result = collections.defaultdict(lambda: {'timestamps': [],
                                              'granularity': [],
                                              'values': []})
    for key in sorted(series, reverse=True):
        combine = numpy.concatenate(series[key])
        # np.unique sorts results for us
        times, indices = numpy.unique(combine['timestamps'],
                                      return_inverse=True)

        # create nd-array (unique series x unique times) and fill
        filler = fill if fill is not None and fill != 'null' else numpy.NaN
        val_grid = numpy.full((len(series[key]), len(times)), filler)
        start = 0
        for i, split in enumerate(series[key]):
            size = len(split)
            val_grid[i][indices[start:start + size]] = split['values']
            start += size
        values = val_grid.T

        if fill is None:
            overlap = numpy.flatnonzero(~numpy.any(numpy.isnan(values),
                                                   axis=1))
            if overlap.size == 0 and needed_percent_of_overlap > 0:
                raise UnAggregableTimeseries('No overlap')
            # if no boundary set, use first/last timestamp which overlap
            if to_timestamp is None and overlap.size:
                times = times[:overlap[-1] + 1]
                values = values[:overlap[-1] + 1]
            if from_timestamp is None and overlap.size:
                times = times[overlap[0]:]
                values = values[overlap[0]:]
            percent_of_overlap = overlap.size * 100.0 / times.size
            if percent_of_overlap < needed_percent_of_overlap:
                raise UnAggregableTimeseries(
                    'Less than %f%% of datapoints overlap in this '
                    'timespan (%.2f%%)' % (needed_percent_of_overlap,
                                           percent_of_overlap))

        granularity, times, values, is_aggregated = (
            agg_operations.evaluate(operations, key, times, values,
                                    False, references[key]))

        if is_aggregated:
            result["aggregated"]["timestamps"].extend(times)
            result["aggregated"]['granularity'].extend([granularity] *
                                                       len(times))
            result["aggregated"]['values'].extend(values.T[0])
        else:
            for i, ref in enumerate(references[key]):
                ident = "%s_%s" % tuple(ref)
                result[ident]["timestamps"].extend(times)
                result[ident]['granularity'].extend([granularity] * len(times))
                result[ident]['values'].extend(values.T[i])

    return dict(((ident, list(six.moves.zip(result[ident]['timestamps'],
                                            result[ident]['granularity'],
                                            result[ident]['values'])))
                 for ident in result))
