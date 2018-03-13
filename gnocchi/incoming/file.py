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
import contextlib
import datetime
import errno
import json
import os
import shutil
import tempfile
import uuid

import daiquiri
import numpy
import six

from gnocchi import incoming
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


class FileStorage(incoming.IncomingDriver):
    def __init__(self, conf, greedy=True):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self.basepath_tmp = os.path.join(self.basepath, 'tmp')

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.basepath))

    def upgrade(self, num_sacks):
        super(FileStorage, self).upgrade(num_sacks)
        utils.ensure_paths([self.basepath_tmp])

    def _get_storage_sacks(self):
        with open(os.path.join(self.basepath_tmp, self.CFG_PREFIX),
                  'r') as f:
            return json.load(f)[self.CFG_SACKS]

    def set_storage_settings(self, num_sacks):
        data = {self.CFG_SACKS: num_sacks}
        with open(os.path.join(self.basepath_tmp, self.CFG_PREFIX), 'w') as f:
            json.dump(data, f)
        utils.ensure_paths((self._sack_path(s) for s in self.iter_sacks()))

    def remove_sacks(self):
        for sack in self.iter_sacks():
            shutil.rmtree(os.path.join(self.basepath, str(sack)))

    def _sack_path(self, sack):
        return os.path.join(self.basepath, str(sack))

    def _measure_path(self, sack, metric_id):
        return os.path.join(self._sack_path(sack), six.text_type(metric_id))

    def _build_measure_path(self, metric_id, random_id=None):
        sack = self.sack_for_metric(metric_id)
        path = self._measure_path(sack, metric_id)
        if random_id:
            if random_id is True:
                now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
                random_id = six.text_type(uuid.uuid4()) + now
            return os.path.join(path, random_id)
        return path

    def _store_new_measures(self, metric_id, data):
        tmpfile = tempfile.NamedTemporaryFile(
            prefix='gnocchi', dir=self.basepath_tmp,
            delete=False)
        tmpfile.write(data)
        tmpfile.close()
        path = self._build_measure_path(metric_id, True)
        while True:
            try:
                os.rename(tmpfile.name, path)
                break
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
                try:
                    os.mkdir(self._build_measure_path(metric_id))
                except OSError as e:
                    # NOTE(jd) It's possible that another process created the
                    # path just before us! In this case, good for us, let's do
                    # nothing then! (see bug #1475684)
                    if e.errno != errno.EEXIST:
                        raise

    def _build_report(self, details):
        report_vars = {'metrics': 0, 'measures': 0, 'metric_details': {}}
        if details:
            def build_metric_report(metric, sack):
                report_vars['metric_details'][metric] = len(
                    self._list_measures_container_for_metric_str(sack, metric))
        else:
            def build_metric_report(metric, sack):
                report_vars['metrics'] += 1
                report_vars['measures'] += len(
                    self._list_measures_container_for_metric_str(sack, metric))

        for sack in self.iter_sacks():
            for metric in set(self._list_target(self._sack_path(sack))):
                build_metric_report(metric, sack)
        return (report_vars['metrics'] or
                len(report_vars['metric_details'].keys()),
                report_vars['measures'] or
                sum(report_vars['metric_details'].values()),
                report_vars['metric_details'] if details else None)

    def _list_measures_container_for_metric_str(self, sack, metric_id):
        return self._list_target(self._measure_path(sack, metric_id))

    def _list_measures_container_for_metric(self, metric_id):
        return self._list_target(self._build_measure_path(metric_id))

    @staticmethod
    def _list_target(target):
        try:
            return os.listdir(target)
        except OSError as e:
            # Some other process treated this one, then do nothing
            if e.errno == errno.ENOENT:
                return []
            raise

    def _delete_measures_files_for_metric(self, metric_id, files):
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

    def delete_unprocessed_measures_for_metric(self, metric_id):
        files = self._list_measures_container_for_metric(metric_id)
        self._delete_measures_files_for_metric(metric_id, files)

    def has_unprocessed(self, metric_id):
        return os.path.isdir(self._build_measure_path(metric_id))

    @contextlib.contextmanager
    def process_measure_for_metrics(self, metric_ids):
        measures = {}
        processed_files = {}
        for metric_id in metric_ids:
            files = self._list_measures_container_for_metric(metric_id)
            processed_files[metric_id] = files
            m = self._make_measures_array()
            for f in files:
                abspath = self._build_measure_path(metric_id, f)
                with open(abspath, "rb") as e:
                    m = numpy.concatenate((
                        m, self._unserialize_measures(f, e.read())))
            measures[metric_id] = m

        yield measures

        for metric_id, files in six.iteritems(processed_files):
            self._delete_measures_files_for_metric(metric_id, files)

    @contextlib.contextmanager
    def process_measures_for_sack(self, sack):
        measures = {}
        processed_files = {}
        for metric_id in self._list_target(self._sack_path(sack)):
            try:
                metric_id = uuid.UUID(metric_id)
            except ValueError:
                LOG.error("Unable to parse %s as an UUID, ignoring metric",
                          metric_id)
                continue
            files = self._list_measures_container_for_metric_str(
                sack, metric_id)
            processed_files[metric_id] = files
            m = self._make_measures_array()
            for f in files:
                abspath = self._build_measure_path(metric_id, f)
                with open(abspath, "rb") as e:
                    m = numpy.concatenate((
                        m, self._unserialize_measures(f, e.read())))
            measures[metric_id] = m

        yield measures

        for metric_id, files in six.iteritems(processed_files):
            self._delete_measures_files_for_metric(metric_id, files)
