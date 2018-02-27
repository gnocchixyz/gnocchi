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

import numbers

import numpy
from numpy.lib.stride_tricks import as_strided

from gnocchi import carbonara
from gnocchi.rest.aggregates import exceptions


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


# TODO(sileht): expose all operators in capability API
binary_operators = {
    u"=": numpy.equal,
    u"==": numpy.equal,
    u"eq": numpy.equal,

    u"<": numpy.less,
    u"lt": numpy.less,

    u">": numpy.greater,
    u"gt": numpy.greater,

    u"<=": numpy.less_equal,
    u"≤": numpy.less_equal,
    u"le": numpy.less_equal,

    u">=": numpy.greater_equal,
    u"≥": numpy.greater_equal,
    u"ge": numpy.greater_equal,

    u"!=": numpy.not_equal,
    u"≠": numpy.not_equal,
    u"ne": numpy.not_equal,

    u"%": numpy.mod,
    u"mod": numpy.mod,

    u"+": numpy.add,
    u"add": numpy.add,

    u"-": numpy.subtract,
    u"sub": numpy.subtract,

    u"*": numpy.multiply,
    u"×": numpy.multiply,
    u"mul": numpy.multiply,

    u"/": numpy.true_divide,
    u"÷": numpy.true_divide,
    u"div": numpy.true_divide,

    u"**": numpy.power,
    u"^": numpy.power,
    u"pow": numpy.power,

}

# TODO(sileht): adds, numpy.around, but it take a decimal argument to handle
unary_operators = {
    u"abs": numpy.absolute,
    u"absolute": numpy.absolute,

    u"neg": numpy.negative,
    u"negative": numpy.negative,

    u"cos": numpy.cos,
    u"sin": numpy.sin,
    u"tan": numpy.tan,
    u"floor": numpy.floor,
    u"ceil": numpy.ceil,
}


unary_operators_with_timestamps = {
    u"rateofchange": lambda t, v: (t[1:], numpy.diff(v.T).T)
}


def handle_unary_operator(nodes, granularity, timestamps, initial_values,
                          is_aggregated, references):
    op = nodes[0]
    granularity, timestamps, values, is_aggregated = evaluate(
        nodes[1], granularity, timestamps, initial_values,
        is_aggregated, references)

    if op in unary_operators:
        values = unary_operators[op](values)
    else:
        timestamps, values = unary_operators_with_timestamps[op](
            timestamps, values)
    return granularity, timestamps, values, is_aggregated


def handle_binary_operator(nodes, granularity, timestamps,
                           initial_values, is_aggregated, references):
    op = nodes[0]
    g1, t1, v1, is_a1 = evaluate(nodes[1], granularity, timestamps,
                                 initial_values, is_aggregated, references)
    g2, t2, v2, is_a2 = evaluate(nodes[2], granularity, timestamps,
                                 initial_values, is_aggregated, references)

    is_aggregated = is_a1 or is_a2
    # We keep the computed timeseries
    if isinstance(v1, numpy.ndarray) and isinstance(v2, numpy.ndarray):
        if not numpy.array_equal(t1, t2) or g1 != g2:
            raise exceptions.UnAggregableTimeseries(
                references,
                "Can't compute timeseries with different "
                "granularity %s <> %s" % (nodes[1], nodes[2]))
        timestamps = t1
        granularity = g1
        is_aggregated = True

    elif isinstance(v2, numpy.ndarray):
        timestamps = t2
        granularity = g2
    else:
        timestamps = t1
        granularity = g1

    values = binary_operators[op](v1, v2)
    return granularity, timestamps, values, is_aggregated


def handle_aggregate(agg, granularity, timestamps, values, is_aggregated,
                     references):
    values = numpy.array([AGG_MAP[agg](values, axis=1)]).T
    if values.shape[1] != 1:
        raise RuntimeError("Unexpected resulting aggregated array shape: %s" %
                           values)
    return (granularity, timestamps, values, True)


def handle_rolling(agg, granularity, timestamps, values, is_aggregated,
                   references, window):
    if window > len(values):
        raise exceptions.UnAggregableTimeseries(
            references,
            "Rolling window '%d' is greater than serie length '%d'" %
            (window, len(values))
        )

    timestamps = timestamps[window - 1:]
    values = values.T
    # rigtorp.se/2011/01/01/rolling-statistics-numpy.html
    shape = values.shape[:-1] + (values.shape[-1] - window + 1, window)
    strides = values.strides + (values.strides[-1],)
    new_values = AGG_MAP[agg](as_strided(values, shape=shape, strides=strides),
                              axis=-1)
    return granularity, timestamps, new_values.T, is_aggregated


def handle_resample(agg, granularity, timestamps, values, is_aggregated,
                    references, sampling):
    # TODO(sileht): make a more optimised version that
    # compute the data across the whole matrix
    new_values = None
    result_timestamps = timestamps
    for ts in values.T:
        ts = carbonara.AggregatedTimeSerie.from_data(
            carbonara.Aggregation(agg, None, None),
            timestamps, ts)
        ts = ts.resample(sampling)
        result_timestamps = ts["timestamps"]
        if new_values is None:
            new_values = numpy.array([ts["values"]])
        else:
            new_values = numpy.concatenate((new_values, [ts["values"]]))
    return sampling, result_timestamps, new_values.T, is_aggregated


def handle_aggregation_operator(nodes, granularity, timestamps, initial_values,
                                is_aggregated, references):
    op = aggregation_operators[nodes[0]]
    agg = nodes[1]
    subnodes = nodes[-1]
    args = nodes[2:-1]
    granularity, timestamps, values, is_aggregated = evaluate(
        subnodes, granularity, timestamps, initial_values,
        is_aggregated, references)
    return op(agg, granularity, timestamps, values, is_aggregated,
              references, *args)


aggregation_operators = {
    u"aggregate": handle_aggregate,
    u"rolling": handle_rolling,
    u"resample": handle_resample,
}


def sanity_check(method):
    # NOTE(sileht): This is important checks, because caller may use zip and
    # build an incomplete timeseries without we notice the result is
    # unexpected.

    def inner(*args, **kwargs):
        granularity, timestamps, values, is_aggregated = method(
            *args, **kwargs)

        t_len = len(timestamps)
        if t_len > 2 and not ((timestamps[1] - timestamps[0]) /
                              granularity).is_integer():
            # NOTE(sileht): numpy.mod is not possible with timedelta64,
            # we don't really care about the remainder value, instead we just
            # check we don't have remainder, by using floor_divide and checking
            # the result is an integer.
            raise RuntimeError("timestamps and granularity doesn't match: "
                               "%s vs %s" % (timestamps[1] - timestamps[0],
                                             granularity))

        elif isinstance(values, numpy.ndarray) and t_len != len(values):
            raise RuntimeError("timestamps and values length are different: "
                               "%s vs %s" % (t_len, len(values)))

        return granularity, timestamps, values, is_aggregated
    return inner


@sanity_check
def evaluate(nodes, granularity, timestamps, initial_values, is_aggregated,
             references):
    if isinstance(nodes, numbers.Number):
        return granularity, timestamps, nodes, is_aggregated
    elif nodes[0] in aggregation_operators:
        return handle_aggregation_operator(nodes, granularity, timestamps,
                                           initial_values, is_aggregated,
                                           references)
    elif nodes[0] in binary_operators:
        return handle_binary_operator(nodes, granularity, timestamps,
                                      initial_values, is_aggregated,
                                      references)
    elif (nodes[0] in unary_operators or
          nodes[0] in unary_operators_with_timestamps):
        return handle_unary_operator(nodes, granularity, timestamps,
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
