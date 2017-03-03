# -*- encoding: utf-8 -*-
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
from collections import defaultdict
import contextlib
import datetime
import functools
import uuid

import six

from gnocchi.storage.common import ceph
from gnocchi.storage.incoming import _carbonara

rados = ceph.rados


class CephStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(CephStorage, self).__init__(conf)
        self.rados, self.ioctx = ceph.create_rados_connection(conf)
        # NOTE(sileht): constants can't be class attributes because
        # they rely on presence of rados module

        # NOTE(sileht): We allow to read the measure object on
        # outdated replicats, that safe for us, we will
        # get the new stuffs on next metricd pass.
        self.OMAP_READ_FLAGS = (rados.LIBRADOS_OPERATION_BALANCE_READS |
                                rados.LIBRADOS_OPERATION_SKIPRWLOCKS)

        # NOTE(sileht): That should be safe to manipulate the omap keys
        # with any OSDs at the same times, each osd should replicate the
        # new key to others and same thing for deletion.
        # I wonder how ceph handle rm_omap and set_omap run at same time
        # on the same key. I assume the operation are timestamped so that will
        # be same. If not, they are still one acceptable race here, a rm_omap
        # can finish before all replicats of set_omap are done, but we don't
        # care, if that occurs next metricd run, will just remove it again, no
        # object with the measure have already been delected by previous, so
        # we are safe and good.
        self.OMAP_WRITE_FLAGS = rados.LIBRADOS_OPERATION_SKIPRWLOCKS

    def stop(self):
        ceph.close_rados_connection(self.rados, self.ioctx)
        super(CephStorage, self).stop()

    def add_measures_batch(self, metrics_and_measures):
        names = []
        for metric, measures in six.iteritems(metrics_and_measures):
            name = "_".join((
                self.MEASURE_PREFIX,
                str(metric.id),
                str(uuid.uuid4()),
                datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")))
            names.append(name)
            data = self._encode_measures(measures)
            self.ioctx.write_full(name, data)

        with rados.WriteOpCtx() as op:
            # NOTE(sileht): list all objects in a pool is too slow with
            # many objects (2min for 20000 objects in 50osds cluster),
            # and enforce us to iterrate over all objects
            # So we create an object MEASURE_PREFIX, that have as
            # omap the list of objects to process (not xattr because
            # it doesn't # allow to configure the locking behavior)
            self.ioctx.set_omap(op, tuple(names), (b"",) * len(names))
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)

    def _build_report(self, details):
        LIMIT = 1000
        metrics = set()
        count = 0
        metric_details = defaultdict(int)
        marker = ""
        while True:
            names = list(self._list_object_names_to_process(marker=marker,
                                                            limit=LIMIT))
            if names and names[0] < marker:
                raise _carbonara.ReportGenerationError("Unable to cleanly "
                                                       "compute backlog.")
            for name in names:
                count += 1
                metric = name.split("_")[1]
                metrics.add(metric)
                if details:
                    metric_details[metric] += 1
            if len(names) < LIMIT:
                break
            else:
                marker = name

        return len(metrics), count, metric_details if details else None

    def _list_object_names_to_process(self, prefix="", marker="", limit=-1):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, marker, prefix, limit)
            try:
                self.ioctx.operate_read_op(
                    op, self.MEASURE_PREFIX, flag=self.OMAP_READ_FLAGS)
            except rados.ObjectNotFound:
                # API have still written nothing
                return ()
            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
                return ()

            return (k for k, v in omaps)

    def list_metric_with_measures_to_process(self, sack):
        names = set()
        marker = ""
        while True:
            obj_names = list(self._list_object_names_to_process(
                marker=marker, limit=1000))
            names.update(name.split("_")[1] for name in obj_names)
            if len(obj_names) < 1000:
                break
            else:
                marker = obj_names[-1]
        return names

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
        object_names = tuple(self._list_object_names_to_process(object_prefix))

        if not object_names:
            return

        for op in list(map(self.ioctx.aio_remove, object_names)):
            op.wait_for_complete_and_cb()

        # Now clean objects and omap
        with rados.WriteOpCtx() as op:
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, object_names)
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric.id)
        object_names = tuple(self._list_object_names_to_process(object_prefix))

        measures = []
        ops = []
        bufsize = 8192  # Same sa rados_read one

        tmp_measures = {}

        def add_to_measures(name, comp, data):
            # Check that the measure file has not been deleted while still
            # listed in the OMAP – this can happen after a crash
            ret = comp.get_return_value()
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
                # Object has been deleted, so this is just a stalled entry
                # in the OMAP listing, ignore
                return

            if name in tmp_measures:
                tmp_measures[name] += data
            else:
                tmp_measures[name] = data
            if len(data) < bufsize:
                measures.extend(self._unserialize_measures(name,
                                                           tmp_measures[name]))
                del tmp_measures[name]
            else:
                ops.append(self.ioctx.aio_read(
                    name, bufsize, len(tmp_measures[name]),
                    functools.partial(add_to_measures, name)
                ))

        for name in object_names:
            ops.append(self.ioctx.aio_read(
                name, bufsize, 0,
                functools.partial(add_to_measures, name)
            ))

        while ops:
            op = ops.pop()
            op.wait_for_complete_and_cb()

        yield measures

        # First delete all objects
        for op in list(map(self.ioctx.aio_remove, object_names)):
            op.wait_for_complete_and_cb()

        # Now clean omap
        with rados.WriteOpCtx() as op:
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, object_names)
            self.ioctx.operate_write_op(op, self.MEASURE_PREFIX,
                                        flags=self.OMAP_WRITE_FLAGS)
