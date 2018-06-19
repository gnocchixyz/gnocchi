# -*- encoding: utf-8 -*-
#
<<<<<<< HEAD
<<<<<<< HEAD
=======
# Copyright © 2018 Red Hat
>>>>>>> 11a2520... api: avoid some indexer queries
=======
# Copyright © 2018 Red Hat
>>>>>>> f21ea84... Add automatic backport labels
# Copyright © 2014-2015 eNovance
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
<<<<<<< HEAD
<<<<<<< HEAD

from oslo_config import cfg

=======
=======
>>>>>>> f21ea84... Add automatic backport labels
import collections

from oslo_config import cfg
import six

from gnocchi import carbonara
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
from gnocchi.common import swift
from gnocchi import storage
from gnocchi import utils

swclient = swift.swclient
swift_utils = swift.swift_utils

OPTS = [
    cfg.StrOpt('swift_auth_version',
               default='1',
               help='Swift authentication version to user.'),
    cfg.BoolOpt('swift_auth_insecure',
                default=False,
                help='If True, swiftclient won\'t check for a valid SSL '
                     'certificate when authenticating.'),
    cfg.StrOpt('swift_url',
               help='Swift URL. '
               'If unset, it is obtained from the auth service.'),
    cfg.StrOpt('swift_authurl',
               default="http://localhost:8080/auth/v1.0",
               help='Swift auth URL.'),
    cfg.StrOpt('swift_preauthtoken',
               secret=True,
               help='Swift token to user to authenticate.'),
    cfg.StrOpt('swift_cacert',
               help='A string giving the CA certificate file to use in '
                    'SSL connections for verifying certs.'),
    cfg.StrOpt('swift_region',
               help='Swift region.'),
    cfg.StrOpt('swift_user',
               default="admin:admin",
               help='Swift user.'),
    cfg.StrOpt('swift_user_domain_name',
               default='Default',
               help='Swift user domain name.'),
    cfg.StrOpt('swift_key',
               secret=True,
               default="admin",
               help='Swift key/password.'),
    cfg.StrOpt('swift_project_name',
               help='Swift tenant name, only used in v2/v3 auth.',
               deprecated_name="swift_tenant_name"),
    cfg.StrOpt('swift_project_domain_name',
               default='Default',
               help='Swift project domain name.'),
    cfg.StrOpt('swift_container_prefix',
               default='gnocchi',
               help='Prefix to namespace metric containers.'),
    cfg.StrOpt('swift_endpoint_type',
               default='publicURL',
               help='Endpoint type to connect to Swift',),
    cfg.StrOpt('swift_service_type',
               default='object-store',
               help='A string giving the service type of the swift service '
                    'to use. This setting is only used if '
                    'swift_auth_version is 2.'),
    cfg.IntOpt('swift_timeout',
               min=0,
               default=300,
               help='Connection timeout in seconds.'),
]


class SwiftStorage(storage.StorageDriver):

    WRITE_FULL = True
    # NOTE(sileht): Using threads with swiftclient doesn't work
    # as expected, so disable it
    MAP_METHOD = staticmethod(utils.sequencial_map)

<<<<<<< HEAD
<<<<<<< HEAD
    def __init__(self, conf, coord=None):
        super(SwiftStorage, self).__init__(conf, coord)
=======
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
>>>>>>> 11a2520... api: avoid some indexer queries
=======
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
>>>>>>> f21ea84... Add automatic backport labels
        self.swift = swift.get_connection(conf)
        self._container_prefix = conf.swift_container_prefix

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self._container_prefix)

    def _container_name(self, metric):
        return '%s.%s' % (self._container_prefix, str(metric.id))

    @staticmethod
    def _object_name(split_key, aggregation, version=3):
        name = '%s_%s_%s' % (
            split_key, aggregation,
            utils.timespan_total_seconds(split_key.sampling),
        )
        return name + '_v%s' % version if version else name

    def _create_metric(self, metric):
        # TODO(jd) A container per user in their account?
        resp = {}
        self.swift.put_container(self._container_name(metric),
                                 response_dict=resp)
        # put_container() should return 201 Created; if it returns 204, that
        # means the metric was already created!
        if resp['status'] == 204:
            raise storage.MetricAlreadyExists(metric)

<<<<<<< HEAD
<<<<<<< HEAD
    def _store_metric_measures(self, metric, key, aggregation,
                               data, offset=None, version=3):
        self.swift.put_object(
            self._container_name(metric),
            self._object_name(key, aggregation, version),
            data)

    def _delete_metric_measures(self, metric, key, aggregation, version=3):
        self.swift.delete_object(
            self._container_name(metric),
            self._object_name(key, aggregation, version))
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
    def _store_metric_splits_unbatched(self, metric, key, aggregation, data,
                                       offset, version):
        self.swift.put_object(
            self._container_name(metric),
            self._object_name(key, aggregation.method, version),
            data)

    def _delete_metric_splits_unbatched(
            self, metric, key, aggregation, version=3):
        self.swift.delete_object(
            self._container_name(metric),
            self._object_name(key, aggregation.method, version))
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels

    def _delete_metric(self, metric):
        container = self._container_name(metric)
        try:
            headers, files = self.swift.get_container(
                container, full_listing=True)
        except swclient.ClientException as e:
            if e.http_status != 404:
                # Maybe it never has been created (no measure)
                raise
        else:
            swift.bulk_delete(self.swift, container, files)
            try:
                self.swift.delete_container(container)
            except swclient.ClientException as e:
                if e.http_status != 404:
                    # Deleted in the meantime? Whatever.
                    raise

<<<<<<< HEAD
<<<<<<< HEAD
    def _get_measures_unbatched(self, metric, key, aggregation, version=3):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), self._object_name(
                    key, aggregation, version))
        except swclient.ClientException as e:
            if e.http_status == 404:
                try:
                    self.swift.head_container(self._container_name(metric))
                except swclient.ClientException as e:
                    if e.http_status == 404:
                        raise storage.MetricDoesNotExist(metric)
                    raise
                raise storage.AggregationDoesNotExist(
                    metric, aggregation, key.sampling)
            raise
        return contents

    def _list_split_keys(self, metric, aggregation, granularity, version=3):
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
    def _get_splits_unbatched(self, metric, key, aggregation, version=3):
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric), self._object_name(
                    key, aggregation.method, version))
        except swclient.ClientException as e:
            if e.http_status == 404:
                return
            raise
        return contents

    def _list_split_keys_unbatched(self, metric, aggregations, version=3):
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
        container = self._container_name(metric)
        try:
            headers, files = self.swift.get_container(
                container, full_listing=True)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
<<<<<<< HEAD
<<<<<<< HEAD
        keys = set()
        granularity = str(utils.timespan_total_seconds(granularity))
        for f in files:
            try:
                meta = f['name'].split('_')
                if (aggregation == meta[1] and granularity == meta[2]
                        and self._version_check(f['name'], version)):
                    keys.add(meta[0])
            except (ValueError, IndexError):
                # Might be "none", or any other file. Be resilient.
                continue
=======
=======
>>>>>>> f21ea84... Add automatic backport labels

        raw_keys = list(map(
            lambda k: k.split("_"),
            (f['name'] for f in files
             if self._version_check(f['name'], version)
             and not f['name'].startswith('none'))))
        keys = collections.defaultdict(set)
        if not raw_keys:
            return keys
        zipped = list(zip(*raw_keys))
        k_timestamps = utils.to_timestamps(zipped[0])
        k_methods = zipped[1]
        k_granularities = list(map(utils.to_timespan, zipped[2]))

        for timestamp, method, granularity in six.moves.zip(
                k_timestamps, k_methods, k_granularities):
            for aggregation in aggregations:
                if (aggregation.method == method
                   and aggregation.granularity == granularity):
                    keys[aggregation].add(carbonara.SplitKey(
                        timestamp,
                        sampling=granularity))
                    break
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
        return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(version):
        return 'none' + ("_v%s" % version if version else "")

<<<<<<< HEAD
<<<<<<< HEAD
    def _get_unaggregated_timeserie(self, metric, version=3):
=======
    def _get_or_create_unaggregated_timeseries_unbatched(
            self, metric, version=3):
>>>>>>> 11a2520... api: avoid some indexer queries
=======
    def _get_or_create_unaggregated_timeseries_unbatched(
            self, metric, version=3):
>>>>>>> f21ea84... Add automatic backport labels
        try:
            headers, contents = self.swift.get_object(
                self._container_name(metric),
                self._build_unaggregated_timeserie_path(version))
        except swclient.ClientException as e:
<<<<<<< HEAD
<<<<<<< HEAD
            if e.http_status == 404:
                raise storage.MetricDoesNotExist(metric)
            raise
        return contents

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self.swift.put_object(self._container_name(metric),
                              self._build_unaggregated_timeserie_path(version),
                              data)
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
            if e.http_status != 404:
                raise
            try:
                self._create_metric(metric)
            except storage.MetricAlreadyExists:
                pass
        else:
            return contents

    def _store_unaggregated_timeseries_unbatched(
            self, metric, data, version=3):
        self.swift.put_object(
            self._container_name(metric),
            self._build_unaggregated_timeserie_path(version),
            data)
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
