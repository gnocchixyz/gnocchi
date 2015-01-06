# -*- encoding: utf-8 -*-
#
# Copyright (c) 2014 eNovance
#
# Author: Julien Danjou <julien@danjou.info>
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
import datetime

import six


class ArchivePolicy(object):
    def __init__(self, name, back_window, definition):
        self.name = name
        self.back_window = back_window
        self.definition = definition

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'],
                   d['back_window'],
                   [ArchivePolicyItem(**definition)
                    for definition in d['definition']])

    def to_human_readable_dict(self):
        return {
            "name": self.name,
            "back_window": self.back_window,
            "definition": [d.to_human_readable_dict()
                           for d in self.definition],
        }


class ArchivePolicyItem(object):
    def __init__(self, granularity=None, points=None, timespan=None):
        if (granularity is not None
           and points is not None
           and timespan is not None):
            if timespan != granularity * points:
                raise ValueError(
                    u"timespan ≠ granularity × points")

        if granularity is None:
            if points is None or timespan is None:
                raise ValueError(
                    "At least two of granularity/points/timespan "
                    "must be provided")
            granularity = round(timespan / float(points))

        if points is None:
            if timespan is None:
                self.timespan = None
            else:
                points = int(timespan / granularity)
                self.timespan = granularity * points
        else:
            self.timespan = granularity * points

        self.points = points
        self.granularity = granularity

    def to_dict(self):
        return {
            'timespan': self.timespan,
            'granularity': self.granularity,
            'points': self.points
        }

    def to_human_readable_dict(self):
        """Return a dict representation with human readable values."""
        return {
            'timespan': six.text_type(
                datetime.timedelta(seconds=self.timespan))
            if self.timespan is not None
            else None,
            'granularity': six.text_type(
                datetime.timedelta(seconds=self.granularity)),
            'points': self.points,
        }
