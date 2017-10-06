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
from gnocchi import storage as gnocchi_storage


LOG = daiquiri.getLogger(__name__)


AGG_MAP = {'mean': numpy.nanmean,
           'median': numpy.nanmedian,
           'std': numpy.nanstd,
           'min': numpy.nanmin,
           'max': numpy.nanmax,
           'sum': numpy.nansum}


class UnAggregableTimeseries(Exception):
    """Error raised when timeseries cannot be aggregated."""
    def __init__(self, reason):
        self.reason = reason
        super(UnAggregableTimeseries, self).__init__(reason)


class MetricUnaggregatable(Exception):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics, reason):
        self.metrics = metrics
        self.reason = reason
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s"
            % (", ".join((str(m.id) for m in metrics)), reason))


def get_cross_metric_measures(storage, metrics, from_timestamp=None,
                              to_timestamp=None, aggregation='mean',
                              reaggregation=None,
                              granularity=None, needed_overlap=100.0,
                              fill=None, transform=None):
    """Get aggregated measures of multiple entities.

    :param storage: The storage driver.
    :param metrics: The metrics measured to aggregate.
    :param from timestamp: The timestamp to get the measure from.
    :param to timestamp: The timestamp to get the measure to.
    :param granularity: The granularity to retrieve.
    :param aggregation: The type of aggregation to retrieve.
    :param reaggregation: The type of aggregation to compute
                          on the retrieved measures.
    :param fill: The value to use to fill in missing data in series.
    :param transform: List of transformation to apply to the series
    """
    for metric in metrics:
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise gnocchi_storage.AggregationDoesNotExist(metric, aggregation)
        if granularity is not None:
            for d in metric.archive_policy.definition:
                if d.granularity == granularity:
                    break
            else:
                raise gnocchi_storage.GranularityDoesNotExist(
                    metric, granularity)

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
            raise MetricUnaggregatable(
                metrics, 'No granularity match')
    else:
        granularities_in_common = [granularity]

    tss = [
        storage._get_measures_timeserie(metric, aggregation, g,
                                        from_timestamp, to_timestamp)
        for metric in metrics
        for g in granularities_in_common
    ]

    if transform is not None:
        tss = list(map(lambda ts: ts.transform(transform), tss))

    try:
        return [(timestamp, r, v) for timestamp, r, v
                in aggregated(tss, reaggregation, from_timestamp,
                              to_timestamp, needed_overlap, fill)]
    except (UnAggregableTimeseries, carbonara.UnknownAggregationMethod) as e:
        raise MetricUnaggregatable(metrics, e.reason)


def aggregated(timeseries, aggregation, from_timestamp=None,
               to_timestamp=None, needed_percent_of_overlap=100.0, fill=None):

    series = collections.defaultdict(list)
    for timeserie in timeseries:
        from_ = (None if from_timestamp is None else
                 carbonara.round_timestamp(from_timestamp, timeserie.sampling))
        series[timeserie.sampling].append(timeserie[from_:to_timestamp])

    result = {'timestamps': [], 'granularity': [], 'values': []}
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

        if aggregation in AGG_MAP:
            values = AGG_MAP[aggregation](values, axis=1)
        elif aggregation == 'count':
            values = numpy.count_nonzero(~numpy.isnan(values), axis=1)
        else:
            raise carbonara.UnknownAggregationMethod(aggregation)

        result['timestamps'].extend(times)
        result['granularity'].extend([key] * len(times))
        result['values'].extend(values)

    return six.moves.zip(result['timestamps'], result['granularity'],
                         result['values'])
