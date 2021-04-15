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

import fnmatch
import itertools
import logging
import numpy
import pecan

from gnocchi import indexer
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import processor
from gnocchi.rest import api
from gnocchi import storage

LOG = logging.getLogger(__name__)


def get_measures_or_abort(references, operations, start,
                          stop, granularity, needed_overlap, fill):
    try:
        return processor.get_measures(
            pecan.request.storage,
            references,
            operations,
            start, stop,
            granularity, needed_overlap, fill)
    except exceptions.UnAggregableTimeseries as e:
        api.abort(400, e)
    # TODO(sileht): We currently got only one metric for these exceptions but
    # we can improve processor to returns all missing metrics at once, so we
    # returns a list for the future
    except storage.MetricDoesNotExist as e:
        api.abort(404, {"cause": "Unknown metrics",
                        "detail": [str(e.metric.id)]})
    except storage.AggregationDoesNotExist as e:
        api.abort(404, {"cause": "Metrics with unknown aggregation",
                        "detail": [(str(e.metric.id), e.method)]})


def _get_measures_by_name(resources, metric_wildcards, operations,
                          start, stop, granularity, needed_overlap, fill,
                          details):
    references = []
    for r in resources:
        references.extend([
            processor.MetricReference(m, agg, r, wildcard)
            for wildcard, agg in metric_wildcards
            for m in r.metrics if fnmatch.fnmatch(m.name, wildcard)
        ])

    if not references:
        raise indexer.NoSuchMetric(set((m for (m, a) in metric_wildcards)))

    response = {
        "measures": get_measures_or_abort(
            references, operations, start, stop, granularity,
            needed_overlap, fill)
    }
    if details:
        response["references"] = set((r.resource for r in references))
    return response


class MeasureGroup(object):
    def __init__(self, group_key, resources):
        self.resources = self.join_sequential_groups(resources)
        self.group_key = dict(group_key)
        self.measures = []
        self.references = None

    def add_measures(self, aggregated_measures, date_map, start, stop,
                     details):
        measures = aggregated_measures['measures']['aggregated']
        if details:
            self.references = aggregated_measures['references']
        for measure in measures:
            self.add_measure(list(measure), date_map, start, stop)

    def add_measure(self, measure, date_map, start, stop):
        measure = Measure(*(measure + [start, stop]))
        if not date_map.get(measure.date, None):
            date_map[measure.date] = []
        date_map[measure.date].append(measure)
        self.measures.append(measure)

    def join_sequential_groups(self, group):
        group.sort(key=lambda key: key['revision_start'])
        new_group = []
        last_it = None
        for it in group:
            if last_it and it['revision_start'] == last_it['revision_end']:
                last_it['revision_end'] = it['revision_end']
                continue

            last_it = it
            new_group.append(it)

        return new_group

    def sum_groups_same_time_values(self):
        to_remove = []
        last_visited = None
        for measure in self.measures:
            if last_visited and last_visited.date == measure.date:
                to_remove.append(last_visited)
                measure.value += last_visited.value

            last_visited = measure

        self.measures = [m for m in self.measures if m not in to_remove]


class Measure(object):
    def __init__(self, date, granularity, value, start, stop):
        self.date = date
        self.granularity = granularity
        self.value = value
        self.start = (numpy.datetime64(start, dtype='datetime64[ns]') if start
                      else date)
        measure_next_window_measurement = self.date + self.granularity
        self.stop = (numpy.datetime64(stop, dtype='datetime64[ns]') if stop
                     else measure_next_window_measurement)
        measure_expected_beg = max(self.start, self.date)
        measure_expected_end = min(self.stop, measure_next_window_measurement)
        measure_delta = measure_expected_end - measure_expected_beg
        measure_delta_ns = measure_delta.astype('timedelta64[ns]')
        self.usage_coefficient = (measure_delta_ns.astype(float) /
                                  self.granularity.astype(float))


class Grouper(object):
    def __init__(self, groups, start, end, body, sorts, attr_filter,
                 references, granularity, needed_overlap, fill, details):
        self.groups = groups
        self.start = start
        self.end = end
        self.body = body
        self.sorts = sorts
        self.attr_filter = attr_filter
        self.references = references
        self.granularity = granularity
        self.needed_overlap = needed_overlap
        self.fill = fill
        self.details = details
        self.measures_date_map = {}
        self.grouped_response = None

    def create_history_period_filter(self):
        period_filter = {
            "and": [
                {
                    "<": {'revision_start': self.end}
                },
                {
                    ">=": {'revision_end': self.start}
                }
            ]
        }
        if not (self.start and self.end):
            return self.attr_filter

        if not self.attr_filter:
            return period_filter

        return {"and": [period_filter, self.attr_filter]}

    def retrieve_resources_history(self):
        return pecan.request.indexer.list_resources(
            self.body["resource_type"],
            attribute_filter=self.create_history_period_filter(),
            history=True,
            sorts=self.sorts)

    def get_grouped_measures(self):
        try:
            resources = self.retrieve_resources_history()
            self.grouped_response = self.get_measures(self.group(resources))
            response = self.format_response()
            LOG.debug("[ Resources History: %s ]", resources)
            LOG.debug("[ Response: %s ]", response)
            return response
        except Exception as e:
            LOG.exception(e)
            raise e

    def group(self, to_group):
        to_group.sort(key=lambda x: tuple((attr, x[attr])
                                          for attr in self.groups))
        grouped_values = \
            itertools.groupby(
                to_group,
                lambda x: tuple((attr, x[attr]) for attr in self.groups))

        grouped = []
        for key, values in grouped_values:
            resources = []
            for value in values:
                self.truncate_resource_time_window(value)
                resources.append(value)
            grouped.append(MeasureGroup(key, resources))

        return grouped

    def truncate_resource_time_window(self, value):
        if self.start:
            if value['revision_start']:
                revision_start = numpy.datetime64(value['revision_start'],
                                                  dtype='datetime64[ns]')
                value['revision_start'] = max(revision_start, self.start)
            else:
                value['revision_start'] = self.start

        if self.end:
            if value['revision_end']:
                revision_end = numpy.datetime64(value['revision_end'],
                                                dtype='datetime64[ns]')
                value['revision_end'] = min(revision_end, self.end)
            else:
                value['revision_end'] = self.end

    def get_measures(self, groups):
        for group in groups:
            first_start = self.start
            date_map = self.get_date_map(group)
            for resource in group.resources:
                start = numpy.datetime64(resource['revision_start'],
                                         dtype='datetime64[ns]') \
                    if resource['revision_start'] else None
                stop = numpy.datetime64(resource['revision_end'],
                                        dtype='datetime64[ns]') \
                    if resource['revision_end'] else None

                LOG.debug("[ Collecting measures from %s to %s ]",
                          start, stop)

                try:
                    if not first_start:
                        first_start = start
                        start = None
                    measure = _get_measures_by_name(
                        [resource], self.references, self.body["operations"],
                        start, stop, self.granularity, self.needed_overlap,
                        self.fill, details=self.details)
                    group.add_measures(measure, date_map, start, stop,
                                       self.details)
                except indexer.NoSuchMetric:
                    continue

        self.truncate_measures()
        self.sum_sequential_group_metrics(groups)
        return groups

    def get_date_map(self, group):
        if 'id' in group.group_key:
            return self.measures_date_map.setdefault(group.group_key['id'], {})

        return self.measures_date_map

    def sum_sequential_group_metrics(self, groups):
        for group in groups:
            group.sum_groups_same_time_values()

    def truncate_measures(self):
        for measures in self.measures_date_map.values():
            if isinstance(measures, list):
                self.truncate_measure([measures])
                continue

            self.truncate_measure(measures.values())

    def truncate_measure(self, measures_list):
        if measures_list and len(measures_list) == 1:
            return
        for measures in measures_list:
            measures_sum = 0
            for measure in measures[:-1]:
                measure_new_val = (measure.value *
                                   measure.usage_coefficient)
                measures_sum += measure_new_val
                measure.value = measure_new_val
            last_measure = measures[-1]
            last_measure.value = abs(measures_sum - last_measure.value)

    def format_response(self):
        measures_list = []
        for group in self.grouped_response:
            aggregated = []
            measures = {
                'measures': {'measures': {'aggregated': aggregated}},
                'group': group.group_key
            }
            if group.references:
                measures['measures']['references'] = group.references

            for measure in group.measures:
                aggregated.append((measure.date, measure.granularity,
                                   measure.value))

            if aggregated:
                measures_list.append(measures)

        return measures_list
