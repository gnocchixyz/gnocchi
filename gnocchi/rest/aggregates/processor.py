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
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import operations as agg_operations
from gnocchi import storage as gnocchi_storage
from gnocchi import utils


LOG = daiquiri.getLogger(__name__)


class MetricReference(object):
    def __init__(self, metric, aggregation, resource=None, wildcard=None):
        self.metric = metric
        self.aggregation = aggregation
        self.resource = resource
        self.timeseries = {}

        if self.resource is None:
            self.name = str(self.metric.id)
        else:
            self.name = self.metric.name

        self.lookup_key = [wildcard or self.name, self.aggregation]

    def __eq__(self, other):
        return (self.metric == other.metric and
                self.resource == other.resource and
                self.aggregation == other.aggregation)


def _get_measures_timeserie(storage, ref, granularity, *args, **kwargs):
    agg = ref.metric.archive_policy.get_aggregation(
        ref.aggregation, granularity)
    try:
        data = storage.get_aggregated_measures(
            {ref.metric: [agg]},
            *args, **kwargs)[ref.metric][agg]
    except gnocchi_storage.MetricDoesNotExist:
        data = carbonara.AggregatedTimeSerie(
            carbonara.Aggregation(ref.aggregation, granularity, None))
    return (ref, data)


def get_measures(storage, references, operations,
                 from_timestamp=None, to_timestamp=None,
                 granularities=None, needed_overlap=100.0,
                 fill=None):
    """Get aggregated measures of multiple entities.

    :param storage: The storage driver.
    :param metrics_and_aggregations: List of metric+agg_method tuple
                                     measured to aggregate.
    :param from timestamp: The timestamp to get the measure from.
    :param to timestamp: The timestamp to get the measure to.
    :param granularities: The granularities to retrieve.
    :param fill: The value to use to fill in missing data in series.
    """

    if granularities is None:
        all_granularities = (
            definition.granularity
            for ref in references
            for definition in ref.metric.archive_policy.definition
        )
        # granularities_in_common
        granularities = [
            g
            for g, occurrence in six.iteritems(
                collections.Counter(all_granularities))
            if occurrence == len(references)
        ]

        if not granularities:
            raise exceptions.UnAggregableTimeseries(
                list((ref.name, ref.aggregation)
                     for ref in references),
                'No granularity match')

    references_with_missing_granularity = []
    for ref in references:
        if (ref.aggregation not in
                ref.metric.archive_policy.aggregation_methods):
            raise gnocchi_storage.AggregationDoesNotExist(
                ref.metric, ref.aggregation,
                # Use the first granularity, that should be good enough since
                # they are all missing anyway
                ref.metric.archive_policy.definition[0].granularity)

        available_granularities = [
            d.granularity
            for d in ref.metric.archive_policy.definition
        ]
        for g in granularities:
            if g not in available_granularities:
                references_with_missing_granularity.append(
                    (ref.name, ref.aggregation, g))
                break

    if references_with_missing_granularity:
        raise exceptions.UnAggregableTimeseries(
            references_with_missing_granularity,
            "Granularities are missing")

    tss = utils.parallel_map(_get_measures_timeserie,
                             [(storage, ref, g, from_timestamp, to_timestamp)
                              for ref in references
                              for g in granularities])

    return aggregated(tss, operations, from_timestamp, to_timestamp,
                      needed_overlap, fill)


def aggregated(refs_and_timeseries, operations, from_timestamp=None,
               to_timestamp=None, needed_percent_of_overlap=100.0, fill=None):

    series = collections.defaultdict(list)
    references = collections.defaultdict(list)
    lookup_keys = collections.defaultdict(list)
    for (ref, timeserie) in refs_and_timeseries:
        from_ = (None if from_timestamp is None else
                 carbonara.round_timestamp(
                     from_timestamp, timeserie.aggregation.granularity))
        references[timeserie.aggregation.granularity].append(ref)
        lookup_keys[timeserie.aggregation.granularity].append(ref.lookup_key)
        series[timeserie.aggregation.granularity].append(
            timeserie[from_:to_timestamp])

    result = []
    is_aggregated = False
    result = {}
    for sampling in sorted(series, reverse=True):
        combine = numpy.concatenate(series[sampling])
        # np.unique sorts results for us
        times, indices = numpy.unique(combine['timestamps'],
                                      return_inverse=True)

        # create nd-array (unique series x unique times) and fill
        filler = (numpy.NaN if fill in [None, 'null', 'dropna']
                  else fill)
        val_grid = numpy.full((len(series[sampling]), len(times)), filler)
        start = 0
        for i, split in enumerate(series[sampling]):
            size = len(split)
            val_grid[i][indices[start:start + size]] = split['values']
            start += size
        values = val_grid.T

        if fill is None:
            overlap = numpy.flatnonzero(~numpy.any(numpy.isnan(values),
                                                   axis=1))
            if overlap.size == 0 and needed_percent_of_overlap > 0:
                raise exceptions.UnAggregableTimeseries(lookup_keys[sampling],
                                                        'No overlap')
            if times.size:
                # if no boundary set, use first/last timestamp which overlap
                if to_timestamp is None and overlap.size:
                    times = times[:overlap[-1] + 1]
                    values = values[:overlap[-1] + 1]
                if from_timestamp is None and overlap.size:
                    times = times[overlap[0]:]
                    values = values[overlap[0]:]
                percent_of_overlap = overlap.size * 100.0 / times.size
                if percent_of_overlap < needed_percent_of_overlap:
                    raise exceptions.UnAggregableTimeseries(
                        lookup_keys[sampling],
                        'Less than %f%% of datapoints overlap in this '
                        'timespan (%.2f%%)' % (needed_percent_of_overlap,
                                               percent_of_overlap))

        granularity, times, values, is_aggregated = (
            agg_operations.evaluate(operations, sampling, times, values,
                                    False, lookup_keys[sampling]))

        values = values.T
        result[sampling] = (granularity, times, values, references[sampling])

    if is_aggregated:
        output = {"aggregated": []}
        for sampling in sorted(result, reverse=True):
            granularity, times, values, references = result[sampling]
            if fill == "dropna":
                pos = ~numpy.isnan(values[0])
                v = values[0][pos]
                t = times[pos]
            else:
                v = values[0]
                t = times
            g = [granularity] * len(t)
            output["aggregated"].extend(six.moves.zip(t, g, v))
        return output
    else:
        r_output = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: collections.defaultdict(list)))
        m_output = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for sampling in sorted(result, reverse=True):
            granularity, times, values, references = result[sampling]
            for i, ref in enumerate(references):
                if fill == "dropna":
                    pos = ~numpy.isnan(values[i])
                    v = values[i][pos]
                    t = times[pos]
                else:
                    v = values[i]
                    t = times
                g = [granularity] * len(t)
                measures = six.moves.zip(t, g, v)
                if ref.resource is None:
                    m_output[ref.name][ref.aggregation].extend(measures)
                else:
                    r_output[str(ref.resource.id)][
                        ref.metric.name][ref.aggregation].extend(measures)
        return r_output if r_output else m_output
