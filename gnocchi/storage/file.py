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
import errno
import os
import shutil
import tempfile

from oslo_config import cfg

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi import utils


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
]


class FileStorage(_carbonara.CarbonaraBasedStorage):
    WRITE_FULL = True

    def __init__(self, conf, incoming, coord=None):
        super(FileStorage, self).__init__(conf, incoming, coord)
        self.basepath = conf.file_basepath
        self.basepath_tmp = os.path.join(self.basepath, 'tmp')
        utils.ensure_paths([self.basepath_tmp])

    def _atomic_file_store(self, dest, data):
        tmpfile = tempfile.NamedTemporaryFile(
            prefix='gnocchi', dir=self.basepath_tmp,
            delete=False)
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

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=3):
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
