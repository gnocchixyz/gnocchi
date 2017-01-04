#!/usr/bin/env python
#
# Copyright (c) 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from gnocchi import utils


WORST_CASE_BYTES_PER_POINT = 8.04


if (len(sys.argv) - 1) % 2 != 0:
    print("Usage: %s <granularity> <timespan> ... <granularity> <timespan>"
          % sys.argv[0])
    sys.exit(1)


def sizeof_fmt(num, suffix='B'):
    for unit in ('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi'):
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


size = 0
for g, t in utils.grouper(sys.argv[1:], 2):
    granularity = utils.to_timespan(g)
    timespan = utils.to_timespan(t)
    points = timespan.total_seconds() / granularity.total_seconds()
    cursize = points * WORST_CASE_BYTES_PER_POINT
    size += cursize
    print("%s over %s = %d points = %s" % (g, t, points, sizeof_fmt(cursize)))

print("Total: " + sizeof_fmt(size))
