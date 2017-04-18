# -*- encoding: utf-8 -*-
#
# Copyright © 2015-2017 Red Hat, Inc.
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

import numpy
import six
import ujson


def to_primitive(obj):
    if isinstance(obj, ((six.text_type,)
                        + six.integer_types
                        + (type(None), bool, float))):
        return obj
    if isinstance(obj, uuid.UUID):
        return six.text_type(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, numpy.datetime64):
        # Do not include nanoseconds if null
        return str(obj).rpartition(".000000000")[0] + "+00:00"
    # This mimics what Pecan implements in its default JSON encoder
    if hasattr(obj, "jsonify"):
        return to_primitive(obj.jsonify())
    if isinstance(obj, dict):
        return {to_primitive(k): to_primitive(v)
                for k, v in obj.items()}
    if hasattr(obj, 'iteritems'):
        return to_primitive(dict(obj.iteritems()))
    # Python 3 does not have iteritems
    if hasattr(obj, 'items'):
        return to_primitive(dict(obj.items()))
    if hasattr(obj, '__iter__'):
        return list(map(to_primitive, obj))
    return obj


def dumps(obj):
    return ujson.dumps(to_primitive(obj))


# For convenience
loads = ujson.loads
load = ujson.load
