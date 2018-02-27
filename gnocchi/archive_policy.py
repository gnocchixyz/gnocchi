# -*- encoding: utf-8 -*-
#
# Copyright (c) 2014-2015 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import datetime
import operator

import numpy
from oslo_config import cfg
from oslo_config import types
import six

from gnocchi import carbonara
from gnocchi import utils


ATTRGETTER_GRANULARITY = operator.attrgetter("granularity")


class ArchivePolicy(object):

    DEFAULT_AGGREGATION_METHODS = ()

    # TODO(eglynn): figure out how to accommodate multi-valued aggregation
    #               methods, where there is no longer just a single aggregate
    #               value to be stored per-period (e.g. ohlc)
    VALID_AGGREGATION_METHODS = set(
        ('mean', 'sum', 'last', 'max', 'min',
         'std', 'median', 'first', 'count')).union(
             set((str(i) + 'pct' for i in six.moves.range(1, 100))))

    VALID_AGGREGATION_METHODS = VALID_AGGREGATION_METHODS.union(
        set(map(lambda s: "rate:" + s,
                VALID_AGGREGATION_METHODS)))

    # Set that contains all the above values + their minus equivalent (-mean)
    # and the "*" entry.
    VALID_AGGREGATION_METHODS_VALUES = VALID_AGGREGATION_METHODS.union(
        set(('*',)),
        set(map(lambda s: "-" + s,
                VALID_AGGREGATION_METHODS)),
        set(map(lambda s: "+" + s,
                VALID_AGGREGATION_METHODS)))

    def __init__(self, name, back_window, definition,
                 aggregation_methods=None):
        self.name = name
        self.back_window = back_window
        self.definition = []
        for d in definition:
            if isinstance(d, ArchivePolicyItem):
                self.definition.append(d)
            elif isinstance(d, dict):
                self.definition.append(ArchivePolicyItem(**d))
            elif len(d) == 2:
                self.definition.append(
                    ArchivePolicyItem(points=d[0], granularity=d[1]))
            else:
                raise ValueError(
                    "Unable to understand policy definition %s" % d)

        duplicate_granularities = [
            granularity
            for granularity, count in collections.Counter(
                d.granularity for d in self.definition).items()
            if count > 1
        ]
        if duplicate_granularities:
            raise ValueError(
                "More than one archive policy "
                "uses granularity `%s'"
                % utils.timespan_total_seconds(duplicate_granularities[0])
            )

        if aggregation_methods is None:
            self.aggregation_methods = self.DEFAULT_AGGREGATION_METHODS
        else:
            self.aggregation_methods = set(aggregation_methods)

    def get_aggregation(self, method, granularity):
        # Find the timespan
        for d in self.definition:
            if d.granularity == granularity:
                return carbonara.Aggregation(
                    method, d.granularity, d.timespan)

    def get_aggregations_for_method(self, method):
        """Return a list of aggregation for a method.

        List is sorted by granularity, desc.

        :param method: Aggregation method.
        """
        return [carbonara.Aggregation(method, d.granularity, d.timespan)
                for d in sorted(self.definition,
                                key=ATTRGETTER_GRANULARITY, reverse=True)]

    @property
    def aggregations(self):
        return [carbonara.Aggregation(method, d.granularity, d.timespan)
                for d in sorted(self.definition, key=ATTRGETTER_GRANULARITY)
                for method in self.aggregation_methods]

    @property
    def aggregation_methods(self):
        if '*' in self._aggregation_methods:
            agg_methods = self.VALID_AGGREGATION_METHODS.copy()
        elif all(map(lambda s: s.startswith('-') or s.startswith('+'),
                     self._aggregation_methods)):
            agg_methods = set(self.DEFAULT_AGGREGATION_METHODS)
        else:
            agg_methods = set(self._aggregation_methods)

        for entry in self._aggregation_methods:
            if entry:
                if entry[0] == '-':
                    agg_methods -= set((entry[1:],))
                elif entry[0] == '+':
                    agg_methods.add(entry[1:])

        return agg_methods

    @aggregation_methods.setter
    def aggregation_methods(self, value):
        value = set(value)
        rest = value - self.VALID_AGGREGATION_METHODS_VALUES
        if rest:
            raise ValueError("Invalid value for aggregation_methods: %s" %
                             rest)
        self._aggregation_methods = value

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'],
                   d['back_window'],
                   d['definition'],
                   d.get('aggregation_methods'))

    def __eq__(self, other):
        return (isinstance(other, ArchivePolicy)
                and self.name == other.name
                and self.back_window == other.back_window
                and self.definition == other.definition
                and self.aggregation_methods == other.aggregation_methods)

    def jsonify(self):
        return {
            "name": self.name,
            "back_window": self.back_window,
            "definition": self.definition,
            "aggregation_methods": self.aggregation_methods,
        }

    @property
    def max_block_size(self):
        # The biggest block size is the coarse grained archive definition
        return sorted(self.definition,
                      key=operator.attrgetter("granularity"))[-1].granularity


OPTS = [
    cfg.ListOpt(
        'default_aggregation_methods',
        item_type=types.String(
            choices=ArchivePolicy.VALID_AGGREGATION_METHODS),
        default=['mean', 'min', 'max', 'sum', 'std', 'count'],
        help='Default aggregation methods to use in created archive policies'),
]


class ArchivePolicyItem(dict):
    def __init__(self, granularity=None, points=None, timespan=None):
        if (granularity is not None
           and points is not None
           and timespan is not None):
            if timespan != granularity * points:
                raise ValueError(
                    u"timespan ≠ granularity × points")

        if granularity is not None:
            if not isinstance(granularity, numpy.timedelta64):
                granularity = numpy.timedelta64(int(granularity * 10e8), 'ns')
            if granularity <= numpy.timedelta64(0, 'ns'):
                raise ValueError("Granularity should be > 0")

        if points is not None and points <= 0:
            raise ValueError("Number of points should be > 0")

        if (timespan is not None
           and not isinstance(timespan, numpy.timedelta64)):
            timespan = numpy.timedelta64(int(timespan * 10e8), 'ns')

        if granularity is None:
            if points is None or timespan is None:
                raise ValueError(
                    "At least two of granularity/points/timespan "
                    "must be provided")
            granularity = timespan / float(points)

        if points is None:
            if timespan is None:
                self['timespan'] = None
            else:
                points = int(timespan / granularity)
                if points <= 0:
                    raise ValueError("Calculated number of points is < 0")
                self['timespan'] = granularity * points
        else:
            points = int(points)
            self['timespan'] = granularity * points

        self['points'] = points
        self['granularity'] = granularity

    @property
    def granularity(self):
        return self['granularity']

    @property
    def points(self):
        return self['points']

    @property
    def timespan(self):
        return self['timespan']

    def jsonify(self):
        """Return a dict representation with human readable values."""
        return {
            'timespan': six.text_type(
                datetime.timedelta(
                    seconds=utils.timespan_total_seconds(
                        self.timespan)))
            if self.timespan is not None
            else None,
            'granularity': six.text_type(
                datetime.timedelta(
                    seconds=utils.timespan_total_seconds(
                        self.granularity))),
            'points': self.points,
        }

    def serialize(self):
        return {
            'timespan': None
            if self.timespan is None
            else float(utils.timespan_total_seconds(self.timespan)),
            'granularity': float(
                utils.timespan_total_seconds(self.granularity)),
            'points': self.points,
        }


DEFAULT_ARCHIVE_POLICIES = {
    'bool': ArchivePolicy(
        "bool", 3600, [
            # 1 second resolution for 365 days
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 's'),
                              timespan=numpy.timedelta64(365, 'D')),
        ],
        aggregation_methods=("last",),
    ),
    'low': ArchivePolicy(
        "low", 0, [
            # 5 minutes resolution for 30 days
            ArchivePolicyItem(granularity=numpy.timedelta64(5, 'm'),
                              timespan=numpy.timedelta64(30, 'D')),
        ],
    ),
    'medium': ArchivePolicy(
        "medium", 0, [
            # 1 minute resolution for 7 days
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 'm'),
                              timespan=numpy.timedelta64(7, 'D')),
            # 1 hour resolution for 365 days
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 'h'),
                              timespan=numpy.timedelta64(365, 'D')),
        ],
    ),
    'high': ArchivePolicy(
        "high", 0, [
            # 1 second resolution for an hour
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 's'),
                              timespan=numpy.timedelta64(1, 'h')),
            # 1 minute resolution for a week
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 'm'),
                              timespan=numpy.timedelta64(7, 'D')),
            # 1 hour resolution for a year
            ArchivePolicyItem(granularity=numpy.timedelta64(1, 'h'),
                              timespan=numpy.timedelta64(365, 'D')),
        ],
    ),
}
