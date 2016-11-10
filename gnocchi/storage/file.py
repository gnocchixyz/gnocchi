# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Objectif Libre
# Copyright © 2015 Red Hat
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
import errno
import os
import shutil
import tempfile
import uuid

from oslo_config import cfg
import six

from gnocchi import storage
from gnocchi.storage import _carbonara


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
    cfg.StrOpt('file_basepath_tmp',
               default='${file_basepath}/tmp',
               help='Path used to store Gnocchi temporary files.'),
]


class FileStorage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = True

    def __init__(self, conf):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self.basepath_tmp = conf.file_basepath_tmp
        try:
            os.mkdir(self.basepath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.measure_path = os.path.join(self.basepath, self.MEASURE_PREFIX)
        try:
            os.mkdir(self.measure_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        try:
            os.mkdir(self.basepath_tmp)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def _get_tempfile(self):
        return tempfile.NamedTemporaryFile(prefix='gnocchi',
                                           dir=self.basepath_tmp,
                                           delete=False)

    def _atomic_file_store(self, dest, data):
        tmpfile = self._get_tempfile()
        tmpfile.write(data)
        tmpfile.close()
        os.rename(tmpfile.name, dest)

    def _build_metric_dir(self, metric):
        return os.path.join(self.basepath, str(metric.id))

    def _build_unaggregated_timeserie_path(self, metric, version=3):
        return os.path.join(
            self._build_metric_dir(metric),
            'none' + ("_v%s" % version if version else ""))

    def _build_metric_path(self, metric, aggregation):
        return os.path.join(self._build_metric_dir(metric),
                            "agg_" + aggregation)

    def _build_metric_path_for_split(self, metric, aggregation,
                                     timestamp_key, granularity, version=3):
        path = os.path.join(self._build_metric_path(metric, aggregation),
                            timestamp_key + "_" + str(granularity))
        return path + '_v%s' % version if version else path

    def _build_measure_path(self, metric_id, random_id=None):
        path = os.path.join(self.measure_path, six.text_type(metric_id))
        if random_id:
            if random_id is True:
                now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
                random_id = six.text_type(uuid.uuid4()) + now
            return os.path.join(path, random_id)
        return path

    def _create_metric(self, metric):
        path = self._build_metric_dir(metric)
        try:
            os.mkdir(path, 0o750)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise storage.MetricAlreadyExists(metric)
            raise
        for agg in metric.archive_policy.aggregation_methods:
            try:
                os.mkdir(self._build_metric_path(metric, agg), 0o750)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def _store_new_measures(self, metric, data):
        tmpfile = self._get_tempfile()
        tmpfile.write(data)
        tmpfile.close()
        path = self._build_measure_path(metric.id, True)
        while True:
            try:
                os.rename(tmpfile.name, path)
                break
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                try:
                    os.mkdir(self._build_measure_path(metric.id))
                except OSError as e:
                    # NOTE(jd) It's possible that another process created the
                    # path just before us! In this case, good for us, let's do
                    # nothing then! (see bug #1475684)
                    if e.errno != errno.EEXIST:
                        raise

    def _build_report(self, details):
        metric_details = {}
        for metric in os.listdir(self.measure_path):
            metric_details[metric] = (
                self._pending_measures_to_process_count(metric))
        return (len(metric_details.keys()), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, size, part, full=False):
        if full:
            return set(os.listdir(self.measure_path))
        return set(
            os.listdir(self.measure_path)[size * part:size * (part + 1)])

    def _list_measures_container_for_metric_id(self, metric_id):
        try:
            return os.listdir(self._build_measure_path(metric_id))
        except OSError as e:
            # Some other process treated this one, then do nothing
            if e.errno == errno.ENOENT:
                return []
            raise

    def _delete_measures_files_for_metric_id(self, metric_id, files):
        for f in files:
            try:
                os.unlink(self._build_measure_path(metric_id, f))
            except OSError as e:
                # Another process deleted it in the meantime, no prob'
                if e.errno != errno.ENOENT:
                    raise
        try:
            os.rmdir(self._build_measure_path(metric_id))
        except OSError as e:
            # ENOENT: ok, it has been removed at almost the same time
            #         by another process
            # ENOTEMPTY: ok, someone pushed measure in the meantime,
            #            we'll delete the measures and directory later
            # EEXIST: some systems use this instead of ENOTEMPTY
            if e.errno not in (errno.ENOENT, errno.ENOTEMPTY, errno.EEXIST):
                raise

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        files = self._list_measures_container_for_metric_id(metric_id)
        self._delete_measures_files_for_metric_id(metric_id, files)

    def _pending_measures_to_process_count(self, metric_id):
        return len(self._list_measures_container_for_metric_id(metric_id))

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        files = self._list_measures_container_for_metric_id(metric.id)
        measures = []
        for f in files:
            abspath = self._build_measure_path(metric.id, f)
            with open(abspath, "rb") as e:
                measures.extend(self._unserialize_measures(f, e.read()))

        yield measures

        self._delete_measures_files_for_metric_id(metric.id, files)

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._atomic_file_store(
            self._build_unaggregated_timeserie_path(metric, version),
            data)

    def _get_unaggregated_timeserie(self, metric, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        try:
            with open(path, 'rb') as f:
                return f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise

    def _delete_unaggregated_timeserie(self, metric, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        try:
            os.unlink(path)
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        try:
            files = os.listdir(self._build_metric_path(metric, aggregation))
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise
        keys = set()
        for f in files:
            meta = f.split("_")
            if meta[1] == str(granularity) and self._version_check(f, version):
                keys.add(meta[0])
        return keys

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        os.unlink(self._build_metric_path_for_split(
            metric, aggregation, timestamp_key, granularity, version))

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=None, version=3):
        self._atomic_file_store(
            self._build_metric_path_for_split(metric, aggregation,
                                              timestamp_key, granularity,
                                              version),
            data)

    def _delete_metric(self, metric):
        path = self._build_metric_dir(metric)
        try:
            shutil.rmtree(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                # NOTE(jd) Maybe the metric has never been created (no
                # measures)
                raise

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        path = self._build_metric_path_for_split(
            metric, aggregation, timestamp_key, granularity, version)
        try:
            with open(path, 'rb') as aggregation_file:
                return aggregation_file.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                if os.path.exists(self._build_metric_dir(metric)):
                    raise storage.AggregationDoesNotExist(metric, aggregation)
                raise storage.MetricDoesNotExist(metric)
            raise
