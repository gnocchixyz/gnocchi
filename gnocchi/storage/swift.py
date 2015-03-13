# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
from oslo.config import cfg
from swiftclient import client as swclient

from gnocchi import storage
from gnocchi.storage import _carbonara


OPTS = [
    cfg.StrOpt('swift_auth_version',
               default='1',
               help='Swift authentication version to user.'),
    cfg.StrOpt('swift_preauthurl',
               default=None,
               help='Swift pre-auth URL.'),
    cfg.StrOpt('swift_authurl',
               default="http://localhost:8080/auth/v1.0",
               help='Swift auth URL.'),
    cfg.StrOpt('swift_preauthtoken',
               default=None,
               help='Swift token to user to authenticate.'),
    cfg.StrOpt('swift_user',
               default="admin:admin",
               help='Swift user.'),
    cfg.StrOpt('swift_key',
               default="admin",
               help='Swift key/password.'),
    cfg.StrOpt('swift_tenant_name',
               help='Swift tenant name, only used in v2 auth.'),
]


class SwiftStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        self.swift = swclient.Connection(
            auth_version=conf.swift_auth_version,
            authurl=conf.swift_authurl,
            preauthtoken=conf.swift_preauthtoken,
            user=conf.swift_user,
            key=conf.swift_key,
            tenant_name=conf.swift_tenant_name)
        self._lock = _carbonara.CarbonaraBasedStorageToozLock(conf)

    def _create_metric_container(self, metric):
        # TODO(jd) A container per user in their account?
        resp = {}
        self.swift.put_container(metric.name, response_dict=resp)
        # put_container() should return 201 Created; if it returns 204, that
        # means the metric was already created!
        if resp['status'] == 204:
            raise storage.MetricAlreadyExists(metric)

    def _store_metric_measures(self, metric, aggregation, data):
        self.swift.put_object(metric.name, aggregation, data)

    def delete_metric(self, metric):
        try:
            for aggregation in metric.archive_policy.aggregation_methods:
                try:
                    self.swift.delete_object(metric.name, aggregation)
                except swclient.ClientException as e:
                    if e.http_status != 404:
                        raise

            self.swift.delete_container(metric.name)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise

    def _get_measures(self, metric, aggregation):
        try:
            headers, contents = self.swift.get_object(metric.name, aggregation)
        except swclient.ClientException as e:
            if e.http_status == 404:
                try:
                    self.swift.head_container(metric.name)
                except swclient.ClientException as e:
                    if e.http_status == 404:
                        raise storage.MetricDoesNotExist(metric)
                    raise
                raise storage.AggregationDoesNotExist(metric, aggregation)
            raise
        return contents
