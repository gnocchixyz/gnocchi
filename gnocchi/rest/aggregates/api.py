# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
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

import daiquiri
import fnmatch
import itertools

import numpy
import pecan
from pecan import rest
import pyparsing
import voluptuous

from gnocchi import indexer
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import operations as agg_operations
from gnocchi.rest.aggregates import processor
from gnocchi.rest import api
from gnocchi import storage
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


def _OperationsSubNodeSchema(v):
    return OperationsSubNodeSchema(v)


def MetricSchema(v):
    """metric keyword schema

    It could be:

    ["metric", "metric-ref", "aggregation"]

    or

    ["metric, ["metric-ref", "aggregation"], ["metric-ref", "aggregation"]]
    """
    if not isinstance(v, (list, tuple)):
        raise voluptuous.Invalid("Expected a tuple/list, got a %s" % type(v))
    elif not v:
        raise voluptuous.Invalid("Operation must not be empty")
    elif len(v) < 2:
        raise voluptuous.Invalid("Operation need at least one argument")
    elif v[0] != u"metric":
        # NOTE(sileht): this error message doesn't looks related to "metric",
        # but because that the last schema validated by voluptuous, we have
        # good chance (voluptuous.Any is not predictable) to print this
        # message even if it's an other operation that invalid.
        raise voluptuous.Invalid("'%s' operation invalid" % v[0])

    return [u"metric"] + voluptuous.Schema(voluptuous.Any(
        voluptuous.ExactSequence([str, str]),
        voluptuous.All(
            voluptuous.Length(min=1),
            [voluptuous.ExactSequence([str, str])],
        )), required=True)(v[1:])


OperationsSchemaBase = [
    MetricSchema,
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.ternary_operators.keys())),
         _OperationsSubNodeSchema, _OperationsSubNodeSchema,
         _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.binary_operators.keys())),
         _OperationsSubNodeSchema, _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.ternary_operators.keys())),
         _OperationsSubNodeSchema, _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.unary_operators.keys())),
         _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.unary_operators_with_timestamps.keys())),
         _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [u"aggregate",
         voluptuous.Any(*list(agg_operations.AGG_MAP.keys())),
         _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [u"resample",
         voluptuous.Any(*list(agg_operations.AGG_MAP.keys())),
         utils.to_timespan, _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [u"rolling",
         voluptuous.Any(*list(agg_operations.AGG_MAP.keys())),
         voluptuous.All(
             voluptuous.Coerce(int),
             voluptuous.Range(min=1),
         ),
         _OperationsSubNodeSchema]
    )
]


OperationsSubNodeSchema = voluptuous.Schema(voluptuous.Any(*tuple(
    OperationsSchemaBase + [voluptuous.Coerce(float)]
)), required=True)


def OperationsSchema(v):
    if isinstance(v, str):
        try:
            v = pyparsing.OneOrMore(
                pyparsing.nestedExpr()).parseString(v).asList()[0]
        except pyparsing.ParseException as e:
            api.abort(400, {"cause": "Invalid operations",
                            "reason": "Fail to parse the operations string",
                            "detail": str(e)})
    return voluptuous.Schema(voluptuous.Any(*OperationsSchemaBase),
                             required=True)(v)


class ReferencesList(list):
    "A very simplified OrderedSet with list interface"

    def append(self, ref):
        if ref not in self:
            super(ReferencesList, self).append(ref)

    def extend(self, refs):
        for ref in refs:
            self.append(ref)


def extract_references(nodes):
    references = ReferencesList()
    if nodes[0] == "metric":
        if isinstance(nodes[1], list):
            for subnodes in nodes[1:]:
                references.append(tuple(subnodes))
        else:
            references.append(tuple(nodes[1:]))
    else:
        for subnodes in nodes[1:]:
            if isinstance(subnodes, list):
                references.extend(extract_references(subnodes))
    return references


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
        group.sort(key=lambda key: key['search_start'])
        new_group = []
        last_it = None
        for it in group:
            if last_it and it['search_start'] == last_it['search_start']:
                last_it['search_en'] = it['search_en']
                continue

            last_it = it
            new_group.append(it)

        return new_group

    def __str__(self):
        return "Group keys [%s]." % self.group_key

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
        self.start = (numpy.datetime64(start) if start
                      else date)
        measure_next_window_measurement = self.date + self.granularity
        self.stop = (numpy.datetime64(stop) if stop
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
                    "or": [
                        {">=": {'revision_end': self.start}},
                        {"=": {'revision_end': None}}
                    ]
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
        resources = self.retrieve_resources_history()
        groups_to_process_metrics = self.group(resources)
        LOG.debug("Groups [%s] to process metrics of resources [%s].",
                  groups_to_process_metrics, resources)

        self.grouped_response = self.get_measures(groups_to_process_metrics)
        LOG.debug("Response grouped [%s] for resources [%s].",
                  self.grouped_response, resources)

        response = self.format_response()
        LOG.debug("Response to be used after group by process and formatting: "
                  "[%s]", response)
        return response

    def group(self, to_group):
        to_group.sort(key=lambda g: g['revision_start'])
        is_first = True
        for value in to_group:
            self.truncate_resource_time_window(value, is_first=is_first)
            is_first = False
        to_group.sort(key=lambda x: tuple((attr, str(x[attr] or ''))
                                          for attr in self.groups))
        grouped_values = \
            itertools.groupby(
                to_group,
                lambda x: tuple((attr, x[attr]) for attr in self.groups))

        grouped = []
        for key, values in grouped_values:
            resources = []
            for value in values:
                resources.append(value)
            grouped.append(MeasureGroup(key, resources))

        return grouped

    def truncate_resource_time_window(self, value, is_first=False):
        LOG.debug("Truncating timewindow for object [%s] is_first [%s].",
                  value, is_first)

        value['search_start'] = value['revision_start']
        value['search_end'] = value['revision_end']

        if is_first:
            value['search_start'] = self.start
        elif self.start:
            if value['search_start']:
                search_start = numpy.datetime64(value['search_start'])
                value['search_start'] = max(search_start, self.start)
            else:
                value['search_start'] = self.start

        if self.end:
            if value['search_end']:
                search_end = numpy.datetime64(value['search_end'])
                value['search_end'] = min(search_end, self.end)
            else:
                value['search_end'] = self.end

        LOG.debug("Timewindow of object [%s] after truncating.", value)

    def get_measures(self, groups):
        for group in groups:
            date_map = self.get_date_map(group)
            for resource in group.resources:
                start = numpy.datetime64(resource['search_start']) \
                    if resource['search_start'] else None
                stop = numpy.datetime64(resource['search_end']) \
                    if resource['search_end'] else None

                LOG.debug("[ Collecting measures from %s to %s ]",
                          start, stop)

                try:
                    measure = AggregatesController._get_measures_by_name(
                        [resource], self.references, self.body["operations"],
                        start, stop, self.granularity, self.needed_overlap,
                        self.fill, details=self.details)

                    LOG.debug(
                        "[%s] measure found for resource [%s], operations "
                        "[%s], start [%s], stop [%s], granularity [%s], "
                        "need_overlap [%s], fill [%s], details [%s], and "
                        "references [%s].", measure, resource,
                        self.body["operations"], start, stop, self.granularity,
                        self.needed_overlap, self.fill, self.details,
                        self.references)

                    group.add_measures(measure, date_map, start, stop,
                                       self.details)
                except indexer.NoSuchMetric as e:
                    LOG.debug("No measure found for resource [%s], operations "
                              "[%s], start [%s], stop [%s], granularity [%s], "
                              "need_overlap [%s], fill [%s], details [%s], "
                              "and references [%s]. Error: [%s].", resource,
                              self.body["operations"], start, stop,
                              self.granularity, self.needed_overlap,
                              self.fill, self.details, self.references, e)
                    continue

        self.truncate_measures()
        self.sum_sequential_group_metrics(groups)
        return groups

    def get_date_map(self, group):
        """This method returns the map used to control the groupby elements.

        If the entry does not exist, an empty map is returned. The rest of the
        code handles/works by adding entries to the map returned by this method
        """
        data_map_key = ""
        all_keys = list(group.group_key.keys())
        if not all_keys:
            LOG.error("A group without keys for [%s] should not be possible.",
                      group)

        return self.measures_date_map
        all_keys.sort()
        for key in all_keys:
            data_map_key += "%s=%s," % (key, group.group_key[key])

        data_map_key = data_map_key[0:len(data_map_key) - 1]

        date_map_to_return = self.measures_date_map.setdefault(data_map_key, {})
        LOG.debug("Date map [%s] found for key [%s].",
                  date_map_to_return, data_map_key)

        return date_map_to_return

    def sum_sequential_group_metrics(self, groups):
        for group in groups:
            group.sum_groups_same_time_values()

    def truncate_measures(self):
        for measures in self.measures_date_map.values():
            if isinstance(measures, list):
                self.truncate_measure(measures)
                continue

            for measure in measures.values():
                self.truncate_measure(measure)

    def truncate_measure(self, measures_list):
        if not measures_list:
            return

        for measure in measures_list:
            measure_new_val = (measure.value * measure.usage_coefficient)
            LOG.debug('Trucating measure [%s] value to [%s].',
                      measure.__dict__, measure_new_val)
            measure.value = measure_new_val

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
            else:
                LOG.debug("No measure found in measures [%s] of group [%s].",
                          group.measures, group)

        return measures_list


def ResourceTypeSchema(resource_type):
    try:
        pecan.request.indexer.get_resource_type(resource_type)
    except indexer.NoSuchResourceType as e:
        api.abort(400, e)
    return resource_type


class AggregatesController(rest.RestController):

    FetchSchema = voluptuous.Any({
        "operations": OperationsSchema
    }, {
        "operations": OperationsSchema,
        "resource_type": ResourceTypeSchema,
        "search": voluptuous.Any(api.ResourceSearchSchema,
                                 api.QueryStringSearchAttrFilter.parse),
    })

    @pecan.expose("json")
    def post(self, start=None, stop=None, granularity=None,
             needed_overlap=None, fill=None, groupby=None, **kwargs):

        use_history = api.get_bool_param('use_history', kwargs)
        details = api.get_bool_param('details', kwargs)

        if fill is None and needed_overlap is None:
            fill = "dropna"
        start, stop, granularity, needed_overlap, fill = api.validate_qs(
            start, stop, granularity, needed_overlap, fill)

        if start:
            start = numpy.datetime64(start)

        if stop:
            stop = numpy.datetime64(stop)

        body = api.deserialize_and_validate(self.FetchSchema)

        references = extract_references(body["operations"])
        if not references:
            api.abort(400, {"cause": "Operations is invalid",
                            "reason": "At least one 'metric' is required",
                            "detail": body["operations"]})

        if "resource_type" in body:
            attr_filter = body["search"]

            time_range_filters = []

            # When we apply the filters in the query, we need to bear in mind
            # that for the queries where use_history=False, we do not have the
            # revision_end as we do not care about revisions.
            # The revision_start exists in the resource history. Therefore,
            # we can leave its use there as well, with use_history=True or not.
            if stop:
                # We use the stop filter as the start value, since we want
                # everything that started until the end of the time range.
                time_range_filters.append({"or": [
                    {"<": {'started_at': stop}},
                    {"<": {'revision_start': stop}}
                ]})

            if start:
                # We want all elements/resources that exist after the start of
                # the time range or that were deleted after the start of the
                # time range.
                time_range_filters.append(
                    {
                        "or": [
                            {">=": {'ended_at': start}},
                            {"=": {'ended_at': None}},
                            {">=": {'revision_end': start}},
                            {"=": {'revision_end': None}}
                        ]
                    } if use_history else {
                        "or": [
                            {">=": {'ended_at': start}},
                            {"=": {'ended_at': None}},
                        ]
                    })
            if not (start and stop):
                LOG.warning("No stop and start filtering provided. Therefore, "
                            "we will use a query that is going to manipulate "
                            "all dataset. This can be quite costly!")

            if time_range_filters:
                if len(time_range_filters) > 1:
                    time_range_filters = {
                        "and": time_range_filters
                    }
                else:
                    time_range_filters = time_range_filters[0]

                if attr_filter:
                    attr_filter = {"and": [time_range_filters, attr_filter]}
                else:
                    attr_filter = time_range_filters

            LOG.debug("Filters to be used in the search query: [%s].",
                      attr_filter)

            policy_filter = (
                pecan.request.auth_helper.get_resource_policy_filter(
                    pecan.request, "search resource", body["resource_type"]))
            if policy_filter:
                if attr_filter:
                    attr_filter = {"and": [
                        policy_filter,
                        attr_filter
                    ]}
                else:
                    attr_filter = policy_filter

            groupby = sorted(set(api.arg_to_list(groupby)))
            sorts = groupby if groupby else api.RESOURCE_DEFAULT_PAGINATION
            results = []
            try:
                if not groupby:
                    resources = pecan.request.indexer.list_resources(
                        body["resource_type"],
                        attribute_filter=attr_filter,
                        sorts=sorts)
                    return self._get_measures_by_name(
                        resources, references, body["operations"], start, stop,
                        granularity, needed_overlap, fill, details=details)

                if use_history:
                    results = self.get_measures_grouping_with_history(
                        attr_filter, body, details, fill, granularity, groupby,
                        needed_overlap, references, sorts, start, stop)

                    LOG.debug("Response of aggregates call with history: "
                              "[%s] for body parameters [%s].", results, body)
                else:
                    resources = pecan.request.indexer.list_resources(
                        body["resource_type"],
                        attribute_filter=attr_filter,
                        sorts=sorts)
                    results = self.get_measures_grouping(
                        body, details, fill, granularity, needed_overlap,
                        references, resources, start, stop, groupby)

                    LOG.debug(
                        "Resources found [%s] with query filter [%s].",
                        [{'original_resource_id': r.original_resource_id,
                          'started_at': r.started_at,
                          'ended_at': r.ended_at} for r in resources or []],
                        attr_filter)

            except indexer.NoSuchMetric as e:
                api.abort(404, str(e))
            except indexer.IndexerException as e:
                api.abort(400, str(e))
            except Exception as e:
                LOG.exception(e)
                raise e

            if not results:
                all_metrics_not_found = list(set((m for (m, a) in references)))
                all_metrics_not_found.sort()
                api.abort(404, str(
                    indexer.NoSuchMetric(all_metrics_not_found)))
            return results

        else:
            try:
                metric_ids = set(str(utils.UUID(m))
                                 for (m, a) in references)
            except ValueError as e:
                api.abort(400, {"cause": "Invalid metric references",
                                "reason": str(e),
                                "detail": references})

            metrics = pecan.request.indexer.list_metrics(
                attribute_filter={"in": {"id": metric_ids}})
            missing_metric_ids = (set(metric_ids)
                                  - set(str(m.id) for m in metrics))
            if missing_metric_ids:
                api.abort(404, {"cause": "Unknown metrics",
                                "reason": "Provided metrics don't exists",
                                "detail": missing_metric_ids})

            number_of_metrics = len(metrics)
            if number_of_metrics == 0:
                return []

            for metric in metrics:
                api.enforce("get metric", metric)

            metrics_by_ids = dict((str(m.id), m) for m in metrics)
            references = [processor.MetricReference(metrics_by_ids[m], a)
                          for (m, a) in references]

            response = {
                "measures": get_measures_or_abort(
                    references, body["operations"],
                    start, stop, granularity, needed_overlap, fill)
            }
            if details:
                response["references"] = metrics

            return response

    def get_measures_grouping(self, body, details, fill, granularity,
                              needed_overlap, references, resources, start,
                              stop, groupby):
        def groupper(r):
            return tuple((attr, r[attr]) for attr in groupby)

        results = []
        for key, resources in itertools.groupby(resources, groupper):
            resources = list(resources)

            LOG.debug("Executing collection of metrics for group [%s] and "
                      "resources [%s].", key, resources)

            try:
                results.append({
                    "group": dict(key),
                    "measures": self._get_measures_by_name(
                        resources, references, body["operations"],
                        start, stop, granularity, needed_overlap, fill,
                        details=details)
                })
            except indexer.NoSuchMetric:
                pass
        return results

    def get_measures_grouping_with_history(self, attr_filter, body, details,
                                           fill, granularity, groupby,
                                           needed_overlap, references,
                                           sorts, start, stop):
        grouper = Grouper(groupby, start, stop, body,
                          sorts, attr_filter, references,
                          granularity, needed_overlap,
                          fill, details)
        return grouper.get_grouped_measures()

    @staticmethod
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
            all_metrics_not_found = list(
                set((m for (m, a) in metric_wildcards)))
            all_metrics_not_found.sort()
            raise indexer.NoSuchMetric(all_metrics_not_found)

        response = {
            "measures": get_measures_or_abort(
                references, operations, start, stop, granularity,
                needed_overlap, fill)
        }
        if details:
            response["references"] = set((r.resource for r in references))
        return response
