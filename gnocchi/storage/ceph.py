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
import logging
import uuid

from oslo_config import cfg
from oslo_utils import importutils

from gnocchi import storage
from gnocchi.storage import _carbonara


LOG = logging.getLogger(__name__)

for RADOS_MODULE_NAME in ('cradox', 'rados'):
    rados = importutils.try_import(RADOS_MODULE_NAME)
    if rados is not None:
        break
else:
    RADOS_MODULE_NAME = None

if rados is not None and hasattr(rados, 'run_in_thread'):
    rados.run_in_thread = lambda target, args, timeout=None: target(*args)
    LOG.info("rados.run_in_thread is monkeypatched.")


OPTS = [
    cfg.StrOpt('ceph_pool',
               default='gnocchi',
               help='Ceph pool name to use.'),
    cfg.StrOpt('ceph_username',
               help='Ceph username (ie: client.admin).'),
    cfg.StrOpt('ceph_keyring',
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

        if not rados:
            raise ImportError("No module named 'rados' nor 'cradox'")

        LOG.info("Ceph storage backend use '%s' python library" %
                 RADOS_MODULE_NAME)

        # NOTE(sileht): librados handles reconnection itself,
        # by default if a call timeout (30sec), it raises
        # a rados.Timeout exception, and librados
        # still continues to reconnect on the next call
        self.rados = rados.Rados(conffile=conf.ceph_conffile,
                                 rados_id=conf.ceph_username,
                                 conf=options)
        self.rados.connect()

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
            datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")))
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)
            ioctx.set_xattr(self.MEASURE_PREFIX, name, "")

    def _list_object_names_to_process(self, ioctx, prefix):
        try:
            xattrs = ioctx.get_xattrs(self.MEASURE_PREFIX)
        except rados.ObjectNotFound:
            return ()
        return (name for name, __ in xattrs if name.startswith(prefix))

    def _pending_measures_to_process_count(self, metric_id):
        with self._get_ioctx() as ioctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            return len(list(self._list_object_names_to_process(ioctx,
                                                               object_prefix)))

    def _list_metric_with_measures_to_process(self, full=False):
        with self._get_ioctx() as ioctx:
            try:
                xattrs = ioctx.get_xattrs(self.MEASURE_PREFIX)
            except rados.ObjectNotFound:
                return []
        metrics = set()
        for name, __ in xattrs:
            metrics.add(name.split("_")[1])
            if (full is False and
               len(metrics) >=
               self.METRIC_WITH_MEASURES_TO_PROCESS_BATCH_SIZE):
                break
        return metrics

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
            object_names = self._list_object_names_to_process(ctx,
                                                              object_prefix)
            for n in object_names:
                try:
                    ctx.rm_xattr(self.MEASURE_PREFIX, n)
                    ctx.remove_object(n)
                except rados.ObjectNotFound:
                    # Another worker may have removed it, don't worry.
                    pass

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        with self._get_ioctx() as ctx:
            object_prefix = self.MEASURE_PREFIX + "_" + str(metric.id)
            object_names = list(self._list_object_names_to_process(
                ctx, object_prefix))

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
    def _get_object_name(metric, timestamp_key, aggregation, granularity):
        return str("gnocchi_%s_%s_%s_%s" % (
            metric.id, timestamp_key, aggregation, granularity))

    @staticmethod
    def _object_exists(ioctx, name):
        try:
            ioctx.stat(name)
            return True
        except rados.ObjectNotFound:
            return False

    def _create_metric(self, metric):
        name = "gnocchi_%s_container" % metric.id
        with self._get_ioctx() as ioctx:
            if self._object_exists(ioctx, name):
                raise storage.MetricAlreadyExists(metric)
            else:
                ioctx.write_full(name, "metric created")

    def _store_metric_measures(self, metric, timestamp_key,
                               aggregation, granularity, data):
        name = self._get_object_name(metric, timestamp_key,
                                     aggregation, granularity)
        with self._get_ioctx() as ioctx:
            ioctx.write_full(name, data)
            ioctx.set_xattr("gnocchi_%s_container" % metric.id, name, "")

    def _delete_metric(self, metric):
        with self._get_ioctx() as ioctx:
            try:
                xattrs = ioctx.get_xattrs("gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                pass
            else:
                for xattr, _ in xattrs:
                    ioctx.remove_object(xattr)
            for name in ('container', 'none'):
                try:
                    ioctx.remove_object("gnocchi_%s_%s" % (metric.id, name))
                except rados.ObjectNotFound:
                    # Maybe it never got measures
                    pass

    def _get_measures(self, metric, timestamp_key, aggregation, granularity):
        try:
            with self._get_ioctx() as ioctx:
                name = self._get_object_name(metric, timestamp_key,
                                             aggregation, granularity)
                return self._get_object_content(ioctx, name)
        except rados.ObjectNotFound:
            with self._get_ioctx() as ioctx:
                if self._object_exists(
                        ioctx, "gnocchi_%s_container" % metric.id):
                    raise storage.AggregationDoesNotExist(metric, aggregation)
                else:
                    raise storage.MetricDoesNotExist(metric)

    def _list_split_keys_for_metric(self, metric, aggregation, granularity):
        with self._get_ioctx() as ioctx:
            try:
                xattrs = ioctx.get_xattrs("gnocchi_%s_container" % metric.id)
            except rados.ObjectNotFound:
                raise storage.MetricDoesNotExist(metric)
            keys = []
            for xattr, value in xattrs:
                _, metric_id, key, agg, g = xattr.split('_', 4)
                if aggregation == agg and granularity == float(g):
                    keys.append(key)

        return keys

    def _get_unaggregated_timeserie(self, metric):
        try:
            with self._get_ioctx() as ioctx:
                return self._get_object_content(
                    ioctx, "gnocchi_%s_none" % metric.id)
        except rados.ObjectNotFound:
            raise storage.MetricDoesNotExist(metric)

    def _store_unaggregated_timeserie(self, metric, data):
        with self._get_ioctx() as ioctx:
            ioctx.write_full("gnocchi_%s_none" % metric.id, data)

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

    # The following methods deal with Gnocchi <= 1.3 archives
    def _get_metric_archive(self, metric, aggregation):
        """Retrieve data in the place we used to store TimeSerieArchive."""
        try:
            with self._get_ioctx() as ioctx:
                return self._get_object_content(
                    ioctx, str("gnocchi_%s_%s" % (metric.id, aggregation)))
        except rados.ObjectNotFound:
            raise storage.AggregationDoesNotExist(metric, aggregation)

    def _store_metric_archive(self, metric, aggregation, data):
        """Stores data in the place we used to store TimeSerieArchive."""
        with self._get_ioctx() as ioctx:
            ioctx.write_full(
                str("gnocchi_%s_%s" % (metric.id, aggregation)), data)

    def _delete_metric_archives(self, metric):
        with self._get_ioctx() as ioctx:
            for aggregation in metric.archive_policy.aggregation_methods:
                try:
                    ioctx.remove_object(
                        str("gnocchi_%s_%s" % (metric.id, aggregation)))
                except rados.ObjectNotFound:
                    pass
