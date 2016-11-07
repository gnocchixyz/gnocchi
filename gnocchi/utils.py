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
import uuid

import iso8601

from oslo_log import log
from oslo_utils import timeutils
from pytimeparse import timeparse
import retrying
import six
from tooz import coordination


LOG = log.getLogger(__name__)

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


def retry_if_retry_is_raised(exception):
    return isinstance(exception, Retry)


# Retry with exponential backoff for up to 1 minute
retry = retrying.retry(wait_exponential_multiplier=500,
                       wait_exponential_max=60000,
                       retry_on_exception=retry_if_retry_is_raised)


# TODO(jd) Move this to tooz?
@retry
def _enable_coordination(coord):
    try:
        coord.start(start_heart=True)
    except Exception as e:
        LOG.error("Unable to start coordinator: %s", e)
        raise Retry(e)


def get_coordinator_and_start(url):
    my_id = str(uuid.uuid4())
    coord = coordination.get_coordinator(url, my_id)
    _enable_coordination(coord)
    return coord, my_id


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
