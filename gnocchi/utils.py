# -*- encoding: utf-8 -*-
#
# Copyright © 2015-2017 Red Hat, Inc.
# Copyright © 2015-2016 eNovance
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
import datetime
import distutils.util
import errno
import itertools
import multiprocessing
import numbers
import os
import uuid

import daiquiri
import iso8601
import monotonic
import numpy
import pandas as pd
import six
import tenacity
from tooz import coordination


LOG = daiquiri.getLogger(__name__)


# uuid5 namespace for id transformation.
# NOTE(chdent): This UUID must stay the same, forever, across all
# of gnocchi to preserve its value as a URN namespace.
RESOURCE_ID_NAMESPACE = uuid.UUID('0a7a15ff-aa13-4ac2-897c-9bdf30ce175b')


def ResourceUUID(value, creator):
    if isinstance(value, uuid.UUID):
        return value
    if '/' in value:
        raise ValueError("'/' is not supported in resource id")
    try:
        return uuid.UUID(value)
    except ValueError:
        if len(value) <= 255:
            if creator is None:
                creator = "\x00"
            # value/creator must be str (unicode) in Python 3 and str (bytes)
            # in Python 2. It's not logical, I know.
            if six.PY2:
                value = value.encode('utf-8')
                creator = creator.encode('utf-8')
            return uuid.uuid5(RESOURCE_ID_NAMESPACE,
                              value + "\x00" + creator)
        raise ValueError(
            'transformable resource id >255 max allowed characters')


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


# Retry with exponential backoff for up to 1 minute
retry = tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.5, max=60),
    # Never retry except when explicitly asked by raising TryAgain
    retry=tenacity.retry_never,
    reraise=True)


# TODO(jd) Move this to tooz?
@retry
def _enable_coordination(coord):
    try:
        coord.start(start_heart=True)
    except Exception as e:
        LOG.error("Unable to start coordinator: %s", e)
        raise tenacity.TryAgain(e)


def get_coordinator_and_start(url):
    my_id = uuid.uuid4().bytes
    coord = coordination.get_coordinator(url, my_id)
    _enable_coordination(coord)
    return coord, my_id


unix_universal_start64 = numpy.datetime64("1970")


def to_timestamps(values):
    try:
        values = list(values)
        if isinstance(values[0], numbers.Real):
            times = pd.to_datetime(values, utc=True, box=False, unit='s')
        elif (isinstance(values[0], datetime.datetime) or
              is_valid_timestamp(values[0])):
            times = pd.to_datetime(values, utc=True, box=False)
        else:
            try:
                float(values[0])
            except ValueError:
                times = (utcnow() + pd.to_timedelta(values)).values
            else:
                times = pd.to_datetime(list(map(float, values)),
                                       utc=True, box=False, unit='s')
    except ValueError:
        raise ValueError("Unable to convert timestamps")

    if (times < unix_universal_start64).any():
        raise ValueError('Timestamp must be after Epoch')

    return times


def is_valid_timestamp(value):
    try:
        pd.to_datetime(value)
    except Exception:
        return False
    return True


def to_timestamp(value):
    return to_timestamps((value,))[0]


def to_datetime(value):
    return timestamp_to_datetime(to_timestamp(value))


def timestamp_to_datetime(v):
    return datetime.datetime.utcfromtimestamp(
        v.astype(float) / 10e8).replace(tzinfo=iso8601.iso8601.UTC)


def to_timespan(value):
    if value is None:
        raise ValueError("Invalid timespan")
    try:
        seconds = float(value)
    except Exception:
        try:
            seconds = pd.to_timedelta(value).total_seconds()
        except Exception:
            raise ValueError("Unable to parse timespan")
    if seconds <= 0:
        raise ValueError("Timespan must be positive")
    return datetime.timedelta(seconds=seconds)


def utcnow():
    """Version of utcnow() that returns utcnow with a correct TZ."""
    return datetime.datetime.now(tz=iso8601.iso8601.UTC)


def normalize_time(timestamp):
    """Normalize time in arbitrary timezone to UTC naive object."""
    offset = timestamp.utcoffset()
    if offset is None:
        return timestamp
    return timestamp.replace(tzinfo=None) - offset


def datetime_utc(*args):
    return datetime.datetime(*args, tzinfo=iso8601.iso8601.UTC)


unix_universal_start = datetime_utc(1970, 1, 1)


def datetime_to_unix(timestamp):
    return (timestamp - unix_universal_start).total_seconds()


def dt_to_unix_ns(*args):
    return int(datetime_to_unix(datetime.datetime(
        *args, tzinfo=iso8601.iso8601.UTC)) * int(10e8))


def dt_in_unix_ns(timestamp):
    return int(datetime_to_unix(timestamp) * int(10e8))


def get_default_workers():
    try:
        default_workers = multiprocessing.cpu_count() or 1
    except NotImplementedError:
        default_workers = 1
    return default_workers


def grouper(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def ensure_paths(paths):
    for p in paths:
        try:
            os.makedirs(p)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


def strtobool(v):
    if isinstance(v, bool):
        return v
    return bool(distutils.util.strtobool(v))


class StopWatch(object):
    """A simple timer/stopwatch helper class.

    Inspired by: apache-commons-lang java stopwatch.

    Not thread-safe (when a single watch is mutated by multiple threads at
    the same time). Thread-safe when used by a single thread (not shared) or
    when operations are performed in a thread-safe manner on these objects by
    wrapping those operations with locks.

    It will use the `monotonic`_ pypi library to find an appropriate
    monotonically increasing time providing function (which typically varies
    depending on operating system and python version).

    .. _monotonic: https://pypi.python.org/pypi/monotonic/
    """
    _STARTED = object()
    _STOPPED = object()

    def __init__(self):
        self._started_at = None
        self._stopped_at = None
        self._state = None

    def start(self):
        """Starts the watch (if not already started).

        NOTE(harlowja): resets any splits previously captured (if any).
        """
        if self._state == self._STARTED:
            return self
        self._started_at = monotonic.monotonic()
        self._state = self._STARTED
        return self

    @staticmethod
    def _delta_seconds(earlier, later):
        # Uses max to avoid the delta/time going backwards (and thus negative).
        return max(0.0, later - earlier)

    def elapsed(self):
        """Returns how many seconds have elapsed."""
        if self._state not in (self._STARTED, self._STOPPED):
            raise RuntimeError("Can not get the elapsed time of a stopwatch"
                               " if it has not been started/stopped")
        if self._state == self._STOPPED:
            elapsed = self._delta_seconds(self._started_at, self._stopped_at)
        else:
            elapsed = self._delta_seconds(
                self._started_at, monotonic.monotonic())
        return elapsed

    def __enter__(self):
        """Starts the watch."""
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        """Stops the watch (ignoring errors if stop fails)."""
        try:
            self.stop()
        except RuntimeError:
            pass

    def stop(self):
        """Stops the watch."""
        if self._state == self._STOPPED:
            return self
        if self._state != self._STARTED:
            raise RuntimeError("Can not stop a stopwatch that has not been"
                               " started")
        self._stopped_at = monotonic.monotonic()
        self._state = self._STOPPED
        return self
