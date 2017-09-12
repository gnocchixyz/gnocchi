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
import iso8601
import pandas
import six

from gnocchi import storage as gnocchi_storage


LOG = daiquiri.getLogger(__name__)


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

    tss = storage._map_in_thread(storage._get_measures_timeserie,
                                 [(metric, aggregation, g,
                                   from_timestamp, to_timestamp)
                                  for metric in metrics
                                  for g in granularities_in_common])

    if transform is not None:
        tss = list(map(lambda ts: ts.transform(transform), tss))

    try:
        return [(timestamp.replace(tzinfo=iso8601.iso8601.UTC), r, v)
                for timestamp, r, v
                in aggregated(tss, reaggregation, from_timestamp, to_timestamp,
                              needed_overlap, fill)]
    except UnAggregableTimeseries as e:
        raise MetricUnaggregatable(metrics, e.reason)


def aggregated(timeseries, aggregation, from_timestamp=None,
               to_timestamp=None, needed_percent_of_overlap=100.0,
               fill=None):
    index = ['timestamp', 'granularity']
    columns = ['timestamp', 'granularity', 'value']
    dataframes = []

    if not timeseries:
        return []

    for timeserie in timeseries:
        timeserie_raw = list(timeserie.fetch(from_timestamp, to_timestamp))

        if timeserie_raw:
            dataframe = pandas.DataFrame(timeserie_raw, columns=columns)
            dataframe = dataframe.set_index(index)
            dataframes.append(dataframe)

    if not dataframes:
        return []

    number_of_distinct_datasource = len(timeseries) / len(
        set(ts.sampling for ts in timeseries)
    )

    left_boundary_ts = None
    right_boundary_ts = None
    if fill is not None:
        fill_df = pandas.concat(dataframes, axis=1)
        if fill != 'null':
            fill_df = fill_df.fillna(fill)
        single_df = pandas.concat([series for __, series in
                                   fill_df.iteritems()]).to_frame()
        grouped = single_df.groupby(level=index)
    else:
        grouped = pandas.concat(dataframes).groupby(level=index)
        maybe_next_timestamp_is_left_boundary = False

        left_holes = 0
        right_holes = 0
        holes = 0
        for (timestamp, __), group in grouped:
            if group.count()['value'] != number_of_distinct_datasource:
                maybe_next_timestamp_is_left_boundary = True
                if left_boundary_ts is not None:
                    right_holes += 1
                else:
                    left_holes += 1
            elif maybe_next_timestamp_is_left_boundary:
                left_boundary_ts = timestamp
                maybe_next_timestamp_is_left_boundary = False
            else:
                right_boundary_ts = timestamp
                holes += right_holes
                right_holes = 0

        if to_timestamp is not None:
            holes += left_holes
        if from_timestamp is not None:
            holes += right_holes

        if to_timestamp is not None or from_timestamp is not None:
            maximum = len(grouped)
            percent_of_overlap = (float(maximum - holes) * 100.0 /
                                  float(maximum))
            if percent_of_overlap < needed_percent_of_overlap:
                raise UnAggregableTimeseries(
                    'Less than %f%% of datapoints overlap in this '
                    'timespan (%.2f%%)' % (needed_percent_of_overlap,
                                           percent_of_overlap))
        if (needed_percent_of_overlap > 0 and
                (right_boundary_ts == left_boundary_ts or
                 (right_boundary_ts is None
                  and maybe_next_timestamp_is_left_boundary))):
            LOG.debug("We didn't find points that overlap in those "
                      "timeseries. "
                      "right_boundary_ts=%(right_boundary_ts)s, "
                      "left_boundary_ts=%(left_boundary_ts)s, "
                      "groups=%(groups)s", {
                          'right_boundary_ts': right_boundary_ts,
                          'left_boundary_ts': left_boundary_ts,
                          'groups': list(grouped)
                      })
            raise UnAggregableTimeseries('No overlap')

    # NOTE(sileht): this call the aggregation method on already
    # aggregated values, for some kind of aggregation this can
    # result can looks weird, but this is the best we can do
    # because we don't have anymore the raw datapoints in those case.
    # FIXME(sileht): so should we bailout is case of stddev, percentile
    # and median?
    agg_timeserie = getattr(grouped, aggregation)()
    agg_timeserie = agg_timeserie.dropna().reset_index()

    if from_timestamp is None and left_boundary_ts:
        agg_timeserie = agg_timeserie[
            agg_timeserie['timestamp'] >= left_boundary_ts]
    if to_timestamp is None and right_boundary_ts:
        agg_timeserie = agg_timeserie[
            agg_timeserie['timestamp'] <= right_boundary_ts]

    points = agg_timeserie.sort_values(by=['granularity', 'timestamp'],
                                       ascending=[0, 1])
    return six.moves.zip(points.timestamp, points.granularity,
                         points.value)
