# -*- encoding: utf-8 -*-
#
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

import contextlib
import datetime
import uuid

from oslo_config import cfg
from oslo_utils import importutils

from gnocchi import storage
from gnocchi.storage import _carbonara

# NOTE(sileht): rados module is not available on pypi
rados = importutils.try_import('rados')

OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               default=None,
               help='Ceph username (ie: client.admin).'),
    cfg.StrOpt('ceph_keyring',
               default=None,
               help='Ceph keyring path.'),
    cfg.StrOpt('ceph_conffile',
               default='/etc/ceph/ceph.conf',
               help='Ceph configuration file.'),
]


class CephStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(CephStorage, self).__init__(conf)
        self.pool = conf.ceph_pool
        self._lock = _carbonara.CarbonaraBasedStorageToozLock(conf)
        options = {}
        if conf.ceph_keyring:
            options['keyring'] = conf.ceph_keyring

        # NOTE(sileht): librados handles reconnection itself,
        # by default if a call timeout (30sec), it raises
        # a rados.Timeout exception, and librados
        # still continues to reconnect on the next call
        self.rados = rados.Rados(conffile=conf.ceph_conffile,
                                 rados_id=conf.ceph_username,
                                 conf=options)
        self.rados.connect()

    def stop(self):
        self._lock.stop()

    def _store_measures(self, metric, data):
        # NOTE(sileht): list all objects in a pool is too slow with
        # many objects (2min for 20000 objects in 50osds cluster),
        # and enforce us to iterrate over all objects
        # So we create an object MEASURE_PREFIX, that have as
        # xattr the list of objects to process
        name = "_".join((
            self.MEASURE_PREFIX,
            str(metric.id),
            str(uuid.uuid4()),
            datetime.datetime.utcnow().strftime("%Y%M%d_%H:%M:%S")))
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)
            ioctx.set_xattr(self.MEASURE_PREFIX, name, "")

    @classmethod
    def _list_object_names_to_process(cls, ioctx, prefix=None):
        try:
            xattrs_iterator = ioctx.get_xattrs(cls.MEASURE_PREFIX)
        except rados.ObjectNotFound:
            return []
        return [name for name, __ in xattrs_iterator
                if prefix is None or name.startswith(prefix)]

    def _pending_measures_to_process_count(self, metric_id):
        with self._get_ioctx() as ioctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            return len(self._list_object_names_to_process(ioctx,
                                                          object_prefix))

    def _list_metric_with_measures_to_process(self):
        with self._get_ioctx() as ioctx:
            return [name.split("_")[1] for name in
                    self._list_object_names_to_process(ioctx)]

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            object_names = self._list_object_names_to_process(ctx,
                                                              object_prefix)
            for n in object_names:
                ctx.rm_xattr(self.MEASURE_PREFIX, n)
                ctx.remove_object(n)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric.id)
            object_names = self._list_object_names_to_process(ctx,
                                                              object_prefix)

            measures = []
            for n in object_names:
                data = self._get_object_content(ctx, n)
                measures.extend(self._unserialize_measures(data))

            yield measures

            # Now clean objects and xattrs
            for n in object_names:
                ctx.rm_xattr(self.MEASURE_PREFIX, n)
                ctx.remove_object(n)

    def _get_ioctx(self):
        return self.rados.open_ioctx(self.pool)

    @staticmethod
    def _get_object_name(metric, lock_name):
        return str("gnocchi_%s_%s" % (metric.id, lock_name))

    @staticmethod
    def _object_exists(ioctx, name):
        try:
            size, mtime = ioctx.stat(name)
            # NOTE(sileht): the object have been created by
            # the lock code
            return size > 0
        except rados.ObjectNotFound:
            return False

    def _create_metric(self, metric):
        name = self._get_object_name(metric, 'container')
        with self._get_ioctx() as ioctx:
            if self._object_exists(ioctx, name):
                raise storage.MetricAlreadyExists(metric)
            else:
                ioctx.write_full(name, "metric created")

    def _store_metric_measures(self, metric, aggregation, data):
        name = self._get_object_name(metric, aggregation)
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)

    def _delete_metric(self, metric):
        with self._get_ioctx() as ioctx:
            for name in ('container', 'none'):
                name = self._get_object_name(metric, name)
                try:
                    ioctx.remove_object(name)
                except rados.ObjectNotFound:
                    # Maybe it never got measures
                    pass
            for aggregation in metric.archive_policy.aggregation_methods:
                name = self._get_object_name(metric, aggregation)
                try:
                    ioctx.remove_object(name)
                except rados.ObjectNotFound:
                    pass

    def _get_measures(self, metric, aggregation):
        try:
            with self._get_ioctx() as ioctx:
                name = self._get_object_name(metric, aggregation)
                content = self._get_object_content(ioctx, name)
                if len(content) == 0:
                    # NOTE(sileht: the object have been created by
                    # the lock code
                    raise rados.ObjectNotFound
                else:
                    return content
        except rados.ObjectNotFound:
            name = self._get_object_name(metric, 'container')
            with self._get_ioctx() as ioctx:
                if self._object_exists(ioctx, name):
                    raise storage.AggregationDoesNotExist(metric, aggregation)
                else:
                    raise storage.MetricDoesNotExist(metric)

    @staticmethod
    def _get_object_content(ioctx, name):
        offset = 0
        content = b''
        while True:
            data = ioctx.read(name, offset=offset)
            if not data:
                break
            content += data
            offset += len(data)
        return content
