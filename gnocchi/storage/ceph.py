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
import errno

from oslo_config import cfg
from oslo_log import log

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage.common import ceph


LOG = log.getLogger(__name__)

OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               help='Ceph username (ie: admin without "client." prefix).'),
    cfg.StrOpt('ceph_secret', help='Ceph key', secret=True),
    cfg.StrOpt('ceph_keyring', help='Ceph keyring path.'),
    cfg.StrOpt('ceph_timeout',
               default="30",
               help='Ceph connection timeout in seconds'),
    cfg.StrOpt('ceph_conffile',
               default='/etc/ceph/ceph.conf',
               help='Ceph configuration file.'),
]

rados = ceph.rados


class CephStorage(_carbonara.CarbonaraBasedStorage):
    WRITE_FULL = False

    def __init__(self, conf, incoming):
        super(CephStorage, self).__init__(conf, incoming)
        self.rados, self.ioctx = ceph.create_rados_connection(conf)

    def stop(self):
        ceph.close_rados_connection(self.rados, self.ioctx)
        super(CephStorage, self).stop()

    def _check_for_metric_upgrade(self, metric):
        lock = self._lock(metric.id)
        with lock:
            container = "gnocchi_%s_container" % metric.id
            unagg_obj = self._build_unaggregated_timeserie_path(metric, 3)
            try:
                xattrs = tuple(k for k, v in self.ioctx.get_xattrs(container))
            except rados.ObjectNotFound:
                # this means already upgraded or some corruption? move on.
                pass
            else:
                # if xattrs are found, it means we're coming from
                # gnocchiv2. migrate to omap accordingly.
                if xattrs:
                    keys = xattrs
                # if no xattrs but object exists, it means it already
                # migrated to v3 and now upgrade to use single object
                else:
                    with rados.ReadOpCtx() as op:
                        omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
                        self.ioctx.operate_read_op(op, container)
                        keys = (k for k, __ in omaps)
                with rados.WriteOpCtx() as op:
                    self.ioctx.set_omap(op, keys,
                                        tuple([b""] * len(keys)))
                    self.ioctx.operate_write_op(op, unagg_obj)
                self.ioctx.remove_object(container)
        super(CephStorage, self)._check_for_metric_upgrade(metric)

    @staticmethod
    def _get_object_name(metric, timestamp_key, aggregation, granularity,
                         version=3):
        name = str("gnocchi_%s_%s_%s_%s" % (
            metric.id, timestamp_key, aggregation, granularity))
        return name + '_v%s' % version if version else name

    def _object_exists(self, name):
        try:
            self.ioctx.stat(name)
            return True
        except rados.ObjectNotFound:
            return False

    def _create_metric(self, metric):
        name = self._build_unaggregated_timeserie_path(metric, 3)
        if self._object_exists(name):
            raise storage.MetricAlreadyExists(metric)
        else:
            self.ioctx.write_full(name, b"")

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity, version)
        if offset is None:
            self.ioctx.write_full(name, data)
        else:
            self.ioctx.write(name, data, offset=offset)
        with rados.WriteOpCtx() as op:
            self.ioctx.set_omap(op, (name,), (b"",))
            self.ioctx.operate_write_op(
                op, self._build_unaggregated_timeserie_path(metric, 3))

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity, version)
        with rados.WriteOpCtx() as op:
            self.ioctx.remove_omap_keys(op, (name,))
            self.ioctx.operate_write_op(
                op, self._build_unaggregated_timeserie_path(metric, 3))
        self.ioctx.aio_remove(name)

    def _delete_metric(self, metric):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, self._build_unaggregated_timeserie_path(metric, 3))
            except rados.ObjectNotFound:
                return
            if ret == errno.ENOENT:
                return
            for name, _ in omaps:
                self.ioctx.aio_remove(name)
        self.ioctx.aio_remove(
            self._build_unaggregated_timeserie_path(metric, 3))

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        try:
            name = self._get_object_name(metric, timestamp_key,
                                         aggregation, granularity, version)
            return self._get_object_content(name)
        except rados.ObjectNotFound:
            if self._object_exists(
                    self._build_unaggregated_timeserie_path(metric, 3)):
                raise storage.AggregationDoesNotExist(metric, aggregation)
            else:
                raise storage.MetricDoesNotExist(metric)

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, self._build_unaggregated_timeserie_path(metric, 3))
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)
            if ret == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            keys = set()
            for name, value in omaps:
                meta = name.split('_')
                if (aggregation == meta[3] and granularity == float(meta[4])
                        and self._version_check(name, version)):
                    keys.add(meta[2])
            return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(metric, version):
        return (('gnocchi_%s_none' % metric.id)
                + ("_v%s" % version if version else ""))

    def _get_unaggregated_timeserie(self, metric, version=3):
        try:
            return self._get_object_content(
                self._build_unaggregated_timeserie_path(metric, version))
        except rados.ObjectNotFound:
            raise storage.MetricDoesNotExist(metric)

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self.ioctx.write_full(
            self._build_unaggregated_timeserie_path(metric, version), data)

    def _delete_unaggregated_timeserie(self, metric, version=3):
        self.ioctx.aio_remove(
            self._build_unaggregated_timeserie_path(metric, version))

    def _get_object_content(self, name):
        offset = 0
        content = b''
        while True:
            data = self.ioctx.read(name, offset=offset)
            if not data:
                break
            content += data
            offset += len(data)
        return content
