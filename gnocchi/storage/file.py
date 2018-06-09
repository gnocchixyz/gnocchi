# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Objectif Libre
# Copyright © 2015-2018 Red Hat
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
import collections
import errno
import itertools
import json
import operator
import os
import shutil
import tempfile
import uuid

import daiquiri
from oslo_config import cfg
import six

from gnocchi import carbonara
from gnocchi import storage
from gnocchi import utils


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
    cfg.IntOpt('file_subdir_len',
               default=2, min=0, max=32,
               help='if > 0, this create a subdirectory for every N bytes'
               'of the metric uuid')
]

ATTRGETTER_METHOD = operator.attrgetter("method")

LOG = daiquiri.getLogger(__name__)

# Python 2 compatibility
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = None


class FileStorage(storage.StorageDriver):
    WRITE_FULL = True
    CFG_PREFIX = 'gnocchi-storage-config'
    CFG_SUBDIR_LEN = 'subdir_len'

    def __init__(self, conf):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self.basepath_tmp = os.path.join(self.basepath, 'tmp')
        self.conf = conf
        self._file_subdir_len = None

    @property
    def SUBDIR_LEN(self):
        if self._file_subdir_len is None:
            config_path = os.path.join(self.basepath_tmp, self.CFG_PREFIX)
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    self._file_subdir_len = json.load(f)[self.CFG_SUBDIR_LEN]
            elif self.is_old_directory_structure():
                self._file_subdir_len = 0
            else:
                # Fresh install
                self._file_subdir_len = self.conf.file_subdir_len

            if self._file_subdir_len != self.conf.file_subdir_len:
                LOG.warning("Changing file_subdir_len is not supported, using "
                            "the stored value: %d", self._file_subdir_len)
        return self._file_subdir_len

    def set_subdir_len(self, subdir_len):
        data = {self.CFG_SUBDIR_LEN: subdir_len}
        with open(os.path.join(self.basepath_tmp, self.CFG_PREFIX), 'w') as f:
            json.dump(data, f)

    def upgrade(self):
        utils.ensure_paths([self.basepath_tmp])
        self.set_subdir_len(self.SUBDIR_LEN)

    def is_old_directory_structure(self):
        # NOTE(sileht): We look for at least one metric directory
        for p in os.listdir(self.basepath):
            if os.path.isdir(p) and '-' in p:
                try:
                    uuid.UUID(p)
                except ValueError:
                    pass
                else:
                    return True
        return False

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.basepath))

    def _atomic_file_store(self, dest, data):
        tmpfile = tempfile.NamedTemporaryFile(
            prefix='gnocchi', dir=self.basepath_tmp,
            delete=False)
        tmpfile.write(data)
        tmpfile.close()
        os.rename(tmpfile.name, dest)

    def _build_metric_dir(self, metric):
        path_parts = [self.basepath]
        if self.SUBDIR_LEN > 0:
            metric_id = metric.id.hex
            path_parts.extend(
                [metric_id[start:start+self.SUBDIR_LEN]
                 for start in range(0, 32, self.SUBDIR_LEN)
                 ])
        path_parts.append(str(metric.id))
        return os.path.join(*path_parts)

    def _build_unaggregated_timeserie_path(self, metric, version=3):
        return os.path.join(
            self._build_metric_dir(metric),
            'none' + ("_v%s" % version if version else ""))

    def _build_metric_path(self, metric, aggregation):
        return os.path.join(self._build_metric_dir(metric),
                            "agg_" + aggregation)

    def _build_metric_path_for_split(self, metric, aggregation,
                                     key, version=3):
        path = os.path.join(
            self._build_metric_path(metric, aggregation),
            str(key)
            + "_"
            + str(utils.timespan_total_seconds(key.sampling)))
        return path + '_v%s' % version if version else path

    def _create_metric(self, metric):
        path = self._build_metric_dir(metric)
        try:
            os.makedirs(path, 0o750)
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

    def _store_unaggregated_timeseries_unbatched(
            self, metric, data, version=3):
        dest = self._build_unaggregated_timeserie_path(metric, version)
        with open(dest, "wb") as f:
            f.write(data)

    def _get_or_create_unaggregated_timeseries_unbatched(
            self, metric, version=3):
        path = self._build_unaggregated_timeserie_path(metric, version)
        try:
            with open(path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            pass
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
        try:
            self._create_metric(metric)
        except storage.MetricAlreadyExists:
            pass

    def _list_split_keys_unbatched(self, metric, aggregations, version=3):
        keys = collections.defaultdict(set)
        for method, grouped_aggregations in itertools.groupby(
                sorted(aggregations, key=ATTRGETTER_METHOD),
                ATTRGETTER_METHOD):
            try:
                files = os.listdir(
                    self._build_metric_path(metric, method))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    raise storage.MetricDoesNotExist(metric)
                raise
            raw_keys = list(map(
                lambda k: k.split("_"),
                filter(
                    lambda f: self._version_check(f, version),
                    files)))
            if not raw_keys:
                continue
            zipped = list(zip(*raw_keys))
            k_timestamps = utils.to_timestamps(zipped[0])
            k_granularities = list(map(utils.to_timespan, zipped[1]))
            grouped_aggregations = list(grouped_aggregations)
            for timestamp, granularity in six.moves.zip(
                    k_timestamps, k_granularities):
                for agg in grouped_aggregations:
                    if granularity == agg.granularity:
                        keys[agg].add(carbonara.SplitKey(
                            timestamp,
                            sampling=granularity))
                        break
        return keys

    def _delete_metric_splits_unbatched(
            self, metric, key, aggregation, version=3):
        os.unlink(self._build_metric_path_for_split(
            metric, aggregation.method, key, version))

    def _store_metric_splits_unbatched(self, metric, key, aggregation, data,
                                       offset, version):
        self._atomic_file_store(
            self._build_metric_path_for_split(
                metric, aggregation.method, key, version),
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

    def _get_splits_unbatched(self, metric, key, aggregation, version=3):
        path = self._build_metric_path_for_split(
            metric, aggregation.method, key, version)
        try:
            with open(path, 'rb') as aggregation_file:
                return aggregation_file.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                return
            raise
