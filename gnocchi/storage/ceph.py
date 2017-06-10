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

from oslo_config import cfg

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage.common import ceph


OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               help='Ceph username (ie: admin without "client." prefix).'),
    cfg.StrOpt('ceph_secret', help='Ceph key', secret=True),
    cfg.StrOpt('ceph_keyring', help='Ceph keyring path.'),
    cfg.IntOpt('ceph_timeout', help='Ceph connection timeout'),
    cfg.StrOpt('ceph_conffile',
               default='/etc/ceph/ceph.conf',
               help='Ceph configuration file.'),
]

rados = ceph.rados


class CephStorage(_carbonara.CarbonaraBasedStorage):
    WRITE_FULL = False

    def __init__(self, conf, incoming, coord=None):
        super(CephStorage, self).__init__(conf, incoming, coord)
        self.rados, self.ioctx = ceph.create_rados_connection(conf)

    def stop(self):
        ceph.close_rados_connection(self.rados, self.ioctx)
        super(CephStorage, self).stop()

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

        try:
            self.ioctx.remove_object(name)
        except rados.ObjectNotFound:
            # It's possible that we already remove that object and then crashed
            # before removing it from the OMAP key list; then no big deal
            # anyway.
            pass

        with rados.WriteOpCtx() as op:
            self.ioctx.remove_omap_keys(op, (name,))
            self.ioctx.operate_write_op(
                op, self._build_unaggregated_timeserie_path(metric, 3))

    def _delete_metric(self, metric):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, self._build_unaggregated_timeserie_path(metric, 3))
            except rados.ObjectNotFound:
                return

            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
                return

        ops = [self.ioctx.aio_remove(name) for name, _ in omaps]

        for op in ops:
            op.wait_for_complete_and_cb()

        try:
            self.ioctx.remove_object(
                self._build_unaggregated_timeserie_path(metric, 3))
        except rados.ObjectNotFound:
            # It's possible that the object does not exists
            pass

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
                                    version=3):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", "", -1)
            try:
                self.ioctx.operate_read_op(
                    op, self._build_unaggregated_timeserie_path(metric, 3))
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)

            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
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
