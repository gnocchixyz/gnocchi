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

import numpy

from gnocchi import carbonara


AGG_MAP = {
    'mean': numpy.nanmean,
    'median': numpy.nanmedian,
    'std': numpy.nanstd,
    'min': numpy.nanmin,
    'max': numpy.nanmax,
    'sum': numpy.nansum,
    'var': numpy.nanvar,
    'count': lambda values, axis: numpy.count_nonzero(
        ~numpy.isnan(values), axis=axis),
}


def handle_aggregate(agg, granularity, timestamps, values, is_aggregated):
    values = numpy.array([AGG_MAP[agg](values, axis=1)]).T
    if values.shape[1] != 1:
        raise RuntimeError("Unexpected resulting aggregated array shape: %s" %
                           values)
    return (granularity, timestamps, values, True)


def handle_aggregation_operator(nodes, granularity, timestamps, initial_values,
                                is_aggregated, references):
    op = aggregation_operators[nodes[0]]
    agg = nodes[1]
    subnodes = nodes[-1]
    args = nodes[2:-1]
    if agg not in AGG_MAP:
        raise carbonara.UnknownAggregationMethod(agg)
    granularity, timestamps, values, is_aggregated = evaluate(
        subnodes, granularity, timestamps, initial_values,
        is_aggregated, references)
    return op(agg, granularity, timestamps, values, is_aggregated, *args)


aggregation_operators = {
    u"aggregate": handle_aggregate,
}


def evaluate(nodes, granularity, timestamps, initial_values, is_aggregated,
             references):
    if nodes[0] in aggregation_operators:
        return handle_aggregation_operator(nodes, granularity, timestamps,
                                           initial_values, is_aggregated,
                                           references)
    elif nodes[0] == "metric":
        if isinstance(nodes[1], list):
            predicat = lambda r: r in nodes[1:]
        else:
            predicat = lambda r: r == nodes[1:]
        indexes = [i for i, r in enumerate(references) if predicat(r)]
        return (granularity, timestamps, initial_values.T[indexes].T,
                is_aggregated)
    else:
        raise RuntimeError("Operation node tree is malformed: %s" % nodes)
