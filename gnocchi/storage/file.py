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
import uuid

from oslo_config import cfg
import six

from gnocchi import storage
from gnocchi.storage import _carbonara


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
]


class FileStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self._lock = _carbonara.CarbonaraBasedStorageToozLock(conf)
        self.measure_path = os.path.join(self.basepath, self.MEASURE_PREFIX)
        try:
            os.mkdir(self.measure_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def stop(self):
        self._lock.stop()

    def _build_metric_path(self, metric, aggregation=None):
        path = os.path.join(self.basepath, str(metric.id))
        if aggregation:
            return os.path.join(path, aggregation)
        return path

    def _build_measure_path(self, metric_id, random_id=None):
        path = os.path.join(self.measure_path, six.text_type(metric_id))
        if random_id:
            if random_id is True:
                now = datetime.datetime.utcnow().strftime("_%Y%M%d_%H:%M:%S")
                random_id = six.text_type(uuid.uuid4()) + now
            return os.path.join(path, random_id)
        return path

    def _create_metric_container(self, metric):
        path = self._build_metric_path(metric)
        try:
            os.mkdir(path, 0o750)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise storage.MetricAlreadyExists(metric)
            raise

    def _store_measures(self, metric, data):
        path = self._build_measure_path(metric.id, True)
        try:
            measure_file = open(path, 'wb')
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            try:
                os.mkdir(self._build_measure_path(metric.id))
            except OSError as e:
                # NOTE(jd) It's possible that another process created the path
                # just before us! In this case, good for us, let's do nothing
                # then! (see bug #1475684)
                if e.errno != errno.EEXIST:
                    raise
            measure_file = open(path, 'wb')
        measure_file.write(data)
        measure_file.close()

    def _list_metric_with_measures_to_process(self):
        return os.listdir(self.measure_path)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        try:
            files = os.listdir(self._build_measure_path(metric.id))
        except OSError as e:
            # Some other process treated this one, then do nothing
            if e.errno == errno.ENOENT:
                yield []
                return
            raise
        measures = []
        for f in files:
            abspath = self._build_measure_path(metric.id, f)
            with open(abspath, "rb") as e:
                measures.extend(self._unserialize_measures(e.read()))

        yield measures

        # Now clean files
        for f in files:
            os.unlink(self._build_measure_path(metric.id, f))

        try:
            os.rmdir(self._build_measure_path(metric.id))
        except OSError as e:
            # New measures have been added, it's ok
            if e.errno != errno.ENOTEMPTY:
                raise

    def _store_metric_measures(self, metric, aggregation, data):
        atomic_path = self._build_metric_path(metric, aggregation)
        path = '%s.tmp' % atomic_path
        with open(path, 'wb') as aggregation_file:
            aggregation_file.write(data)
        os.rename(path, atomic_path)

    def delete_metric(self, metric):
        path = self._build_metric_path(metric)
        try:
            shutil.rmtree(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise storage.MetricDoesNotExist(metric)
            raise
        try:
            shutil.rmtree(os.path.join(self.measure_path,
                                       six.text_type(metric.id)))
        except OSError as e:
            # This metric may have never received any measure
            if e.errno != errno.ENOENT:
                raise

    def _get_measures(self, metric, aggregation):
        path = self._build_metric_path(metric, aggregation)
        try:
            with open(path, 'rb') as aggregation_file:
                return aggregation_file.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                if os.path.exists(self._build_metric_path(metric)):
                    raise storage.AggregationDoesNotExist(metric, aggregation)
                else:
                    raise storage.MetricDoesNotExist(metric)
            raise
