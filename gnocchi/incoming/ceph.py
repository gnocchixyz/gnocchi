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
<<<<<<< HEAD
<<<<<<< HEAD
=======
import daiquiri
>>>>>>> 11a2520... api: avoid some indexer queries
=======
import daiquiri
>>>>>>> f21ea84... Add automatic backport labels
import datetime
import json
import uuid

import numpy
import six

from gnocchi.common import ceph
from gnocchi import incoming

rados = ceph.rados

<<<<<<< HEAD
<<<<<<< HEAD
=======
LOG = daiquiri.getLogger(__name__)

>>>>>>> 11a2520... api: avoid some indexer queries
=======
LOG = daiquiri.getLogger(__name__)

>>>>>>> f21ea84... Add automatic backport labels

class CephStorage(incoming.IncomingDriver):

    Q_LIMIT = 1000

    def __init__(self, conf, greedy=True):
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

    def __str__(self):
        # Use cluster ID for now
        return "%s: %s" % (self.__class__.__name__, self.rados.get_fsid())

    def stop(self):
        ceph.close_rados_connection(self.rados, self.ioctx)
        super(CephStorage, self).stop()

    def _get_storage_sacks(self):
        return json.loads(
            self.ioctx.read(self.CFG_PREFIX).decode())[self.CFG_SACKS]

    def set_storage_settings(self, num_sacks):
        self.ioctx.write_full(self.CFG_PREFIX,
                              json.dumps({self.CFG_SACKS: num_sacks}).encode())

<<<<<<< HEAD
<<<<<<< HEAD
    def remove_sack_group(self, num_sacks):
        prefix = self.get_sack_prefix(num_sacks)
        for i in six.moves.xrange(num_sacks):
            try:
                self.ioctx.remove_object(prefix % i)
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
    def remove_sacks(self):
        for sack in self.iter_sacks():
            try:
                self.ioctx.remove_object(str(sack))
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
            except rados.ObjectNotFound:
                pass

    def add_measures_batch(self, metrics_and_measures):
        data_by_sack = defaultdict(lambda: defaultdict(list))
        for metric_id, measures in six.iteritems(metrics_and_measures):
            name = "_".join((
                self.MEASURE_PREFIX,
                str(metric_id),
                str(uuid.uuid4()),
                datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")))
<<<<<<< HEAD
<<<<<<< HEAD
            sack = self.get_sack_name(self.sack_for_metric(metric_id))
=======
            sack = self.sack_for_metric(metric_id)
>>>>>>> 11a2520... api: avoid some indexer queries
=======
            sack = self.sack_for_metric(metric_id)
>>>>>>> f21ea84... Add automatic backport labels
            data_by_sack[sack]['names'].append(name)
            data_by_sack[sack]['measures'].append(
                self._encode_measures(measures))

        ops = []
        for sack, data in data_by_sack.items():
            with rados.WriteOpCtx() as op:
                # NOTE(sileht): list all objects in a pool is too slow with
                # many objects (2min for 20000 objects in 50osds cluster),
                # and enforce us to iterrate over all objects
                # So we create an object MEASURE_PREFIX, that have as
                # omap the list of objects to process (not xattr because
                # it doesn't # allow to configure the locking behavior)
                self.ioctx.set_omap(op, tuple(data['names']),
                                    tuple(data['measures']))
                ops.append(self.ioctx.operate_aio_write_op(
<<<<<<< HEAD
<<<<<<< HEAD
                    op, sack, flags=self.OMAP_WRITE_FLAGS))
=======
                    op, str(sack), flags=self.OMAP_WRITE_FLAGS))
>>>>>>> 11a2520... api: avoid some indexer queries
=======
                    op, str(sack), flags=self.OMAP_WRITE_FLAGS))
>>>>>>> f21ea84... Add automatic backport labels
        while ops:
            op = ops.pop()
            op.wait_for_complete()

    def _build_report(self, details):
        metrics = set()
        count = 0
        metric_details = defaultdict(int)
<<<<<<< HEAD
<<<<<<< HEAD
        for i in six.moves.range(self.NUM_SACKS):
            marker = ""
            while True:
                names = list(self._list_keys_to_process(
                    i, marker=marker, limit=self.Q_LIMIT))
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
        for sack in self.iter_sacks():
            marker = ""
            while True:
                names = list(self._list_keys_to_process(
                    sack, marker=marker, limit=self.Q_LIMIT))
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
                if names and names[0] < marker:
                    raise incoming.ReportGenerationError(
                        "Unable to cleanly compute backlog.")
                for name in names:
                    count += 1
                    metric = name.split("_")[1]
                    metrics.add(metric)
                    if details:
                        metric_details[metric] += 1
                if len(names) < self.Q_LIMIT:
                    break
                else:
                    marker = name

        return len(metrics), count, metric_details if details else None

    def _list_keys_to_process(self, sack, prefix="", marker="", limit=-1):
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, marker, prefix, limit)
            try:
                self.ioctx.operate_read_op(
<<<<<<< HEAD
<<<<<<< HEAD
                    op, self.get_sack_name(sack), flag=self.OMAP_READ_FLAGS)
            except rados.ObjectNotFound:
                # API have still written nothing
                return ()
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
                    op, str(sack), flag=self.OMAP_READ_FLAGS)
            except rados.ObjectNotFound:
                # API have still written nothing
                return {}
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
<<<<<<< HEAD
<<<<<<< HEAD
                return ()

            return (k for k, v in omaps)

    def list_metric_with_measures_to_process(self, sack):
        names = set()
        marker = ""
        while True:
            obj_names = list(self._list_keys_to_process(
                sack, marker=marker, limit=self.Q_LIMIT))
            names.update(name.split("_")[1] for name in obj_names)
            if len(obj_names) < self.Q_LIMIT:
                break
            else:
                marker = obj_names[-1]
        return names
=======
                return {}

            return dict(omaps)
>>>>>>> 11a2520... api: avoid some indexer queries
=======
                return {}

            return dict(omaps)
>>>>>>> f21ea84... Add automatic backport labels

    def delete_unprocessed_measures_for_metric(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        key_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
<<<<<<< HEAD
<<<<<<< HEAD
        keys = tuple(self._list_keys_to_process(sack, key_prefix))
=======
        keys = tuple(self._list_keys_to_process(sack, key_prefix).keys())
>>>>>>> 11a2520... api: avoid some indexer queries
=======
        keys = tuple(self._list_keys_to_process(sack, key_prefix).keys())
>>>>>>> f21ea84... Add automatic backport labels

        if not keys:
            return

        # Now clean objects and omap
        with rados.WriteOpCtx() as op:
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, keys)
<<<<<<< HEAD
<<<<<<< HEAD
            self.ioctx.operate_write_op(op, self.get_sack_name(sack),
=======
            self.ioctx.operate_write_op(op, str(sack),
>>>>>>> 11a2520... api: avoid some indexer queries
=======
            self.ioctx.operate_write_op(op, str(sack),
>>>>>>> f21ea84... Add automatic backport labels
                                        flags=self.OMAP_WRITE_FLAGS)

    def has_unprocessed(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        object_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)
        return bool(self._list_keys_to_process(sack, object_prefix))

    @contextlib.contextmanager
<<<<<<< HEAD
<<<<<<< HEAD
    def process_measure_for_metric(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        key_prefix = self.MEASURE_PREFIX + "_" + str(metric_id)

        processed_keys = []
        with rados.ReadOpCtx() as op:
            omaps, ret = self.ioctx.get_omap_vals(op, "", key_prefix, -1)
            self.ioctx.operate_read_op(op, self.get_sack_name(sack),
                                       flag=self.OMAP_READ_FLAGS)
            # NOTE(sileht): after reading the libradospy, I'm
            # not sure that ret will have the correct value
            # get_omap_vals transforms the C int to python int
            # before operate_read_op is called, I dunno if the int
            # content is copied during this transformation or if
            # this is a pointer to the C int, I think it's copied...
            try:
                ceph.errno_to_exception(ret)
            except rados.ObjectNotFound:
                # Object has been deleted, so this is just a stalled entry
                # in the OMAP listing, ignore
                return

        measures = self._make_measures_array()
        for k, v in omaps:
            measures = numpy.concatenate(
                (measures, self._unserialize_measures(k, v)))
            processed_keys.append(k)
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
    def process_measure_for_metrics(self, metric_ids):
        measures = {}
        processed_keys = {}
        with rados.ReadOpCtx() as op:
            for metric_id in metric_ids:
                sack = self.sack_for_metric(metric_id)
                processed_keys[sack] = self._list_keys_to_process(
                    sack, prefix=self.MEASURE_PREFIX + "_" + str(metric_id))
                m = self._make_measures_array()
                for k, v in six.iteritems(processed_keys[sack]):
                    m = numpy.concatenate(
                        (m, self._unserialize_measures(k, v)))

                measures[metric_id] = m
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels

        yield measures

        # Now clean omap
        with rados.WriteOpCtx() as op:
<<<<<<< HEAD
<<<<<<< HEAD
            # NOTE(sileht): come on Ceph, no return code
            # for this operation ?!!
            self.ioctx.remove_omap_keys(op, tuple(processed_keys))
            self.ioctx.operate_write_op(op, self.get_sack_name(sack),
                                        flags=self.OMAP_WRITE_FLAGS)
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
            for sack, keys in six.iteritems(processed_keys):
                # NOTE(sileht): come on Ceph, no return code
                # for this operation ?!!
                self.ioctx.remove_omap_keys(op, tuple(keys.keys()))
                self.ioctx.operate_write_op(op, str(sack),
                                            flags=self.OMAP_WRITE_FLAGS)

    @contextlib.contextmanager
    def process_measures_for_sack(self, sack):
        measures = defaultdict(self._make_measures_array)
        omaps = self._list_keys_to_process(
            sack, prefix=self.MEASURE_PREFIX + "_")
        for k, v in six.iteritems(omaps):
            try:
                metric_id = uuid.UUID(k.split("_")[1])
            except (ValueError, IndexError):
                LOG.warning("Unable to parse measure object name %s",
                            k)
                continue
            measures[metric_id] = numpy.concatenate(
                (measures[metric_id], self._unserialize_measures(k, v))
            )

        yield measures

        # Now clean omap
        processed_keys = tuple(omaps.keys())
        if processed_keys:
            with rados.WriteOpCtx() as op:
                # NOTE(sileht): come on Ceph, no return code
                # for this operation ?!!
                self.ioctx.remove_omap_keys(op, tuple(processed_keys))
                self.ioctx.operate_write_op(op, str(sack),
                                            flags=self.OMAP_WRITE_FLAGS)
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
