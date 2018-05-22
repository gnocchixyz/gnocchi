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
import os
import uuid

from concurrent import futures
import daiquiri
import iso8601
import monotonic
import numpy
import pytimeparse
import six
from stevedore import driver
import tenacity


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


unix_universal_start64 = numpy.datetime64("1970")


def to_timestamps(values):
    try:
        if len(values) == 0:
            return []
        if isinstance(values[0], (numpy.datetime64, datetime.datetime)):
            times = numpy.array(values)
        else:
            try:
                # Try to convert to float. If it works, then we consider
                # timestamps to be number of seconds since Epoch
                # e.g. 123456 or 129491.1293
                float(values[0])
            except ValueError:
                try:
                    # Try to parse the value as a string of ISO timestamp
                    # e.g. 2017-10-09T23:23:12.123
                    numpy.datetime64(values[0])
                except ValueError:
                    # Last chance: it can be relative timestamp, so convert
                    # to timedelta relative to now()
                    # e.g. "-10 seconds" or "5 minutes"
                    times = numpy.fromiter(
                        numpy.add(numpy.datetime64(utcnow()),
                                  [to_timespan(v, True) for v in values]),
                        dtype='datetime64[ns]', count=len(values))
                else:
                    times = numpy.array(values, dtype='datetime64[ns]')
            else:
                times = numpy.array(values, dtype='float') * 10e8
    except ValueError:
        raise ValueError("Unable to convert timestamps")

    times = times.astype('datetime64[ns]')

    if (times < unix_universal_start64).any():
        raise ValueError('Timestamp must be after Epoch')

    return times


def to_timestamp(value):
    return to_timestamps([value])[0]


def to_datetime(value):
    return timestamp_to_datetime(to_timestamp(value))


def timestamp_to_datetime(v):
    return datetime.datetime.utcfromtimestamp(
        v.astype(float) / 10e8).replace(tzinfo=iso8601.iso8601.UTC)


def to_timespan(value, allow_le_zero=False):
    if value is None:
        raise ValueError("Invalid timespan")
    try:
        seconds = float(value)
    except Exception:
        seconds = pytimeparse.parse(value)
        if seconds is None:
            raise ValueError("Unable to parse timespan")
    seconds = numpy.timedelta64(int(seconds * 10e8), 'ns')
    if not allow_le_zero and seconds <= numpy.timedelta64(0, 'ns'):
        raise ValueError("Timespan must be positive")
    return seconds


_ONE_SECOND = numpy.timedelta64(1, 's')


def timespan_total_seconds(td):
    return td / _ONE_SECOND


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

    def reset(self):
        """Stop and re-start the watch."""
        self.stop()
        return self.start()


def get_driver_class(namespace, conf):
    """Return the storage driver class.

    :param conf: The conf to use to determine the driver.
    """
    return driver.DriverManager(namespace,
                                conf.driver).driver


def sequencial_map(fn, list_of_args):
    return list(itertools.starmap(fn, list_of_args))


def parallel_map(fn, list_of_args):
    """Run a function in parallel."""

    if parallel_map.MAX_WORKERS == 1:
        return sequencial_map(fn, list_of_args)

    with futures.ThreadPoolExecutor(
            max_workers=parallel_map.MAX_WORKERS) as executor:
        # We use 'list' to iterate all threads here to raise the first
        # exception now, not much choice
        return list(executor.map(lambda args: fn(*args), list_of_args))


parallel_map.MAX_WORKERS = get_default_workers()


def return_none_on_failure(f):
    try:
        # Python 3
        fname = f.__qualname__
    except AttributeError:
        fname = f.__name__

    @six.wraps(f)
    def _return_none_on_failure(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            LOG.critical("Unexpected error while calling %s: %s",
                         fname, e, exc_info=True)

    return _return_none_on_failure


# Retry with exponential backoff for up to 1 minute
wait_exponential = tenacity.wait_exponential(multiplier=0.5, max=60)

retry_on_exception = tenacity.Retrying(wait=wait_exponential)


class _retry_on_exception_and_log(tenacity.retry_if_exception_type):
    def __init__(self, msg):
        super(_retry_on_exception_and_log, self).__init__()
        self.msg = msg

    def __call__(self, attempt):
        if attempt.failed:
            LOG.error(self.msg, exc_info=attempt.exception())
        return super(_retry_on_exception_and_log, self).__call__(attempt)


def retry_on_exception_and_log(msg):
    return tenacity.Retrying(
        wait=wait_exponential, retry=_retry_on_exception_and_log(msg)).wraps
