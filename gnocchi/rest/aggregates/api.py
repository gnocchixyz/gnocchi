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

import pecan
from pecan import rest
import pyparsing
import six
import voluptuous

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
    if not isinstance(v, (list, tuple)) or len(v) <= 2 or v[0] != u"metric":
        raise voluptuous.Invalid("'metric' is invalid")
    return [u"metric"] + voluptuous.Schema(voluptuous.Any(
        voluptuous.ExactSequence([six.text_type, six.text_type]),
        voluptuous.All(
            voluptuous.Length(min=1),
            [voluptuous.ExactSequence([six.text_type, six.text_type])],
        )), required=True)(v[1:])


OperationsSchemaBase = [
    MetricSchema,
    voluptuous.ExactSequence(
        [u"aggregate",
         voluptuous.Any(*list(agg_operations.AGG_MAP.keys())),
         _OperationsSubNodeSchema]
    ),
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
    except processor.UnAggregableTimeseries as e:
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


class FetchController(rest.RestController):

    FetchSchema = {
        "operations": OperationsSchema
    }

    @pecan.expose("json")
    def post(self, start=None, stop=None, granularity=None,
             needed_overlap=100.0, fill=None):
        start, stop, granularity, needed_overlap, fill = api.validate_qs(
            start, stop, granularity, needed_overlap, fill)

        body = api.deserialize_and_validate(self.FetchSchema)

        references = list(extract_references(body["operations"]))
        if not references:
            api.abort(400, {"cause": "operations is invalid",
                            "reason": "at least one 'metric' is required",
                            "detail": body["operations"]})

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


class AggregatesController(object):
    fetch = FetchController()
