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

import itertools

import pecan
from pecan import rest
import pyparsing
import six
import voluptuous

from gnocchi import indexer
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import operations as agg_operations
from gnocchi.rest.aggregates import processor
from gnocchi.rest import api
from gnocchi import storage
from gnocchi import utils


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
        voluptuous.ExactSequence([six.text_type, six.text_type]),
        voluptuous.All(
            voluptuous.Length(min=1),
            [voluptuous.ExactSequence([six.text_type, six.text_type])],
        )), required=True)(v[1:])


OperationsSchemaBase = [
    MetricSchema,
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.binary_operators.keys())),
         _OperationsSubNodeSchema, _OperationsSubNodeSchema]
    ),
    voluptuous.ExactSequence(
        [voluptuous.Any(*list(
            agg_operations.unary_operators.keys())),
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
    if isinstance(v, six.text_type):
        try:
            v = pyparsing.OneOrMore(
                pyparsing.nestedExpr()).parseString(v).asList()[0]
        except pyparsing.ParseException as e:
            api.abort(400, {"cause": "Invalid operations",
                            "reason": "Fail to parse the operations string",
                            "detail": six.text_type(e)})
    return voluptuous.Schema(voluptuous.Any(*OperationsSchemaBase),
                             required=True)(v)


def extract_references(nodes):
    references = set()
    if nodes[0] == "metric":
        if isinstance(nodes[1], list):
            for subnodes in nodes[1:]:
                references.add(tuple(subnodes))
        else:
            references.add(tuple(nodes[1:]))
    else:
        for subnodes in nodes[1:]:
            if isinstance(subnodes, list):
                references |= extract_references(subnodes)
    return references


def get_measures_or_abort(metrics_and_aggregations, operations, start,
                          stop, granularity, needed_overlap, fill,
                          ref_identifier):
    try:
        return processor.get_measures(
            pecan.request.storage,
            metrics_and_aggregations,
            operations,
            start, stop,
            granularity, needed_overlap, fill,
            ref_identifier=ref_identifier)
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
             needed_overlap=100.0, fill=None, groupby=None):
        start, stop, granularity, needed_overlap, fill = api.validate_qs(
            start, stop, granularity, needed_overlap, fill)

        body = api.deserialize_and_validate(self.FetchSchema)

        references = list(extract_references(body["operations"]))
        if not references:
            api.abort(400, {"cause": "Operations is invalid",
                            "reason": "At least one 'metric' is required",
                            "detail": body["operations"]})

        if "resource_type" in body:
            attr_filter = body["search"]
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
            resources = pecan.request.indexer.list_resources(
                body["resource_type"],
                attribute_filter=attr_filter,
                sorts=groupby)
            if not groupby:
                return self._get_measures_by_name(
                    resources, references, body["operations"], start, stop,
                    granularity, needed_overlap, fill)

            def groupper(r):
                return tuple((attr, r[attr]) for attr in groupby)

            results = []
            for key, resources in itertools.groupby(resources, groupper):
                results.append({
                    "group": dict(key),
                    "measures": self._get_measures_by_name(
                        resources, references, body["operations"], start, stop,
                        granularity, needed_overlap, fill)
                })
            return results

        else:
            try:
                metric_ids = [six.text_type(utils.UUID(m))
                              for (m, a) in references]
            except ValueError as e:
                api.abort(400, {"cause": "Invalid metric references",
                                "reason": six.text_type(e),
                                "detail": references})

            metrics = pecan.request.indexer.list_metrics(ids=metric_ids)
            missing_metric_ids = (set(metric_ids)
                                  - set(six.text_type(m.id) for m in metrics))
            if missing_metric_ids:
                api.abort(404, {"cause": "Unknown metrics",
                                "reason": "Provided metrics don't exists",
                                "detail": missing_metric_ids})

            number_of_metrics = len(metrics)
            if number_of_metrics == 0:
                return []

            for metric in metrics:
                api.enforce("get metric", metric)

            metrics_by_ids = dict((six.text_type(m.id), m) for m in metrics)
            metrics_and_aggregations = [(metrics_by_ids[m], a)
                                        for (m, a) in references]
            return get_measures_or_abort(
                metrics_and_aggregations, body["operations"],
                start, stop, granularity, needed_overlap, fill,
                ref_identifier="id")

    def _get_measures_by_name(self, resources, metric_names, operations,
                              start, stop, granularity, needed_overlap, fill):

        metrics_and_aggregations = list(filter(
            lambda x: x[0] is not None, ([r.get_metric(metric_name), agg]
                                         for (metric_name, agg) in metric_names
                                         for r in resources)))
        if not metrics_and_aggregations:
            api.abort(400, {"cause": "Metrics not found",
                            "detail": set((m for (m, a) in metric_names))})

        return get_measures_or_abort(metrics_and_aggregations, operations,
                                     start, stop, granularity, needed_overlap,
                                     fill, ref_identifier="name")
