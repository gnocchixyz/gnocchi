# -*- encoding: utf-8 -*-
#
# Copyright © 2015-2016 Red Hat, Inc.
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

import numpy
from oslo_serialization import jsonutils
import ujson


_ORIG_TP = jsonutils.to_primitive


def _to_primitive(value, *args, **kwargs):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, numpy.datetime64):
        # Do not include nanoseconds if null
        return str(value).rpartition(".000000000")[0] + "+00:00"
    # This mimics what Pecan implements in its default JSON encoder
    if hasattr(value, "jsonify"):
        return _to_primitive(value.jsonify(), *args, **kwargs)
    return _ORIG_TP(value, *args, **kwargs)


def to_primitive(*args, **kwargs):
    try:
        jsonutils.to_primitive = _to_primitive
        return jsonutils.to_primitive(*args, **kwargs)
    finally:
        jsonutils.to_primitive = _ORIG_TP


def dumps(obj, *args, **kwargs):
    return jsonutils.dumps(obj, default=to_primitive)


# For convenience
loads = ujson.loads
load = ujson.load
