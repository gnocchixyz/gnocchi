# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Mehdi Abaakouk <mehdi.abaakouk@enovance.com>
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
import ctypes
import errno
import time

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

    @contextlib.contextmanager
    def _lock(self, metric, lock_name):
        # NOTE(sileht): current stable python binding (0.80.X) doesn't
        # have rados_lock_XXX method, so do ourself the call with ctypes
        #
        # https://github.com/ceph/ceph/commit/f5bf75fa4109b6449a88c7ffdce343cf4691a4f9
        # When ^^ is released, we can drop this code and directly use:
        # - ctx.lock_exclusive(name, 'lock', 'gnocchi')
        # - ctx.unlock(name, 'lock', 'gnocchi')
        name = self._get_object_name(metric, lock_name)
        with self._get_ioctx() as ctx:
            while True:
                ret = rados.run_in_thread(
                    ctx.librados.rados_lock_exclusive,
                    (ctx.io, ctypes.c_char_p(name.encode('ascii')),
                     ctypes.c_char_p(b"lock"),
                     ctypes.c_char_p(b"gnocchi"),
                     ctypes.c_char_p(b""), None, ctypes.c_int8(0)))
                if ret in [errno.EBUSY, errno.EEXIST]:
                    time.sleep(0.1)
                elif ret < 0:
                    rados.make_ex(ret, "Error while getting lock of %s" % name)
                else:
                    break
            try:
                yield
            finally:
                ret = rados.run_in_thread(
                    ctx.librados.rados_unlock,
                    (ctx.io, ctypes.c_char_p(name.encode('ascii')),
                     ctypes.c_char_p(b"lock"), ctypes.c_char_p(b"gnocchi")))
                if ret < 0:
                    rados.make_ex(ret,
                                  "Error while releasing lock of %s" % name)

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
        return True

    def _create_metric_container(self, metric):
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

    def delete_metric(self, metric):
        with self._get_ioctx() as ioctx:
            name = self._get_object_name(metric, 'container')
            try:
                ioctx.remove_object(name)
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)
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
                offset = 0
                content = b''
                while True:
                    data = ioctx.read(name, offset=offset)
                    if not data:
                        break
                    content += data
                    offset += len(content)
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
