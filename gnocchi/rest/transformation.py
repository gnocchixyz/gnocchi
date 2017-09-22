# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
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

import functools

import pyparsing as pp

from gnocchi import carbonara
from gnocchi import utils


# NOTE(sileht): setName is used to make clear error message without pyparsing
# object name
def transform(name, *args):
    parser = pp.Keyword(name)
    args_parser = pp.Suppress("(").setName("(")
    first = True
    for arg in args:
        if not first:
            args_parser += pp.Suppress(",").setName(",")
        args_parser += arg
        first = False
    args_parser += pp.Suppress(")").setName(")")
    if not args:
        args_parser = pp.Optional(args_parser)
    parser = parser + pp.Group(args_parser)
    return parser.setParseAction(
        lambda t: carbonara.Transformation(t[0], tuple(t[1])))


# NOTE(sileht): not sure pp.nums + "." is enough to support all
# pandas.to_timedelta() formats
timespan = pp.Word(pp.nums + ".").setName("timespan")
timespan = timespan.setParseAction(lambda t: utils.to_timespan(t[0]))

absolute = transform("absolute")
negative = transform("negative")
resample = transform("resample", timespan)
rolling = transform("rolling", pp.Word(pp.alphas), pp.Word(pp.nums))

transform = pp.delimitedList(
    absolute | negative | resample | rolling,
    delim=":")

parse = functools.partial(transform.parseString, parseAll=True)
TransformationParserError = pp.ParseException
