# -*- encoding: utf-8 -*-
#
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
import multiprocessing

import iso8601
from oslo_utils import timeutils
from pytimeparse import timeparse
import six
import tenacity
import uuid

# uuid5 namespace for id transformation.
# NOTE(chdent): This UUID must stay the same, forever, across all
# of gnocchi to preserve its value as a URN namespace.
RESOURCE_ID_NAMESPACE = uuid.UUID('0a7a15ff-aa13-4ac2-897c-9bdf30ce175b')


def ResourceUUID(value):
    try:
        try:
            return uuid.UUID(value)
        except ValueError:
            if len(value) <= 255:
                if six.PY2:
                    value = value.encode('utf-8')
                return uuid.uuid5(RESOURCE_ID_NAMESPACE, value)
            raise ValueError(
                'transformable resource id >255 max allowed characters')
    except Exception as e:
        raise ValueError(e)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


class Retry(Exception):
    pass


# Retry with exponential backoff for up to 1 minute
retry = tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.5, max=60),
    retry=tenacity.retry_if_exception_type(Retry),
    reraise=True)


def to_timestamp(v):
    if isinstance(v, datetime.datetime):
        return v
    try:
        v = float(v)
    except (ValueError, TypeError):
        v = six.text_type(v)
        try:
            return timeutils.parse_isotime(v)
        except ValueError:
            delta = timeparse.timeparse(v)
            if delta is None:
                raise ValueError("Unable to parse timestamp %s" % v)
            return utcnow() + datetime.timedelta(seconds=delta)
    return datetime.datetime.utcfromtimestamp(v).replace(
        tzinfo=iso8601.iso8601.UTC)


def to_timespan(value):
    if value is None:
        raise ValueError("Invalid timespan")
    try:
        seconds = int(value)
    except Exception:
        try:
            seconds = timeparse.timeparse(six.text_type(value))
        except Exception:
            raise ValueError("Unable to parse timespan")
    if seconds is None:
        raise ValueError("Unable to parse timespan")
    if seconds <= 0:
        raise ValueError("Timespan must be positive")
    return datetime.timedelta(seconds=seconds)


def utcnow():
    """Better version of utcnow() that returns utcnow with a correct TZ."""
    return timeutils.utcnow(True)


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
