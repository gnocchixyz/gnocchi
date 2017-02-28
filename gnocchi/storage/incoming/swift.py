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
import uuid

import six

from gnocchi.storage.common import swift
from gnocchi.storage.incoming import _carbonara

swclient = swift.swclient
swift_utils = swift.swift_utils


class SwiftStorage(_carbonara.CarbonaraBasedStorage):
    def __init__(self, conf):
        super(SwiftStorage, self).__init__(conf)
        self.swift = swift.get_connection(conf)

    def upgrade(self, indexer):
        super(SwiftStorage, self).upgrade(indexer)
        self.swift.put_container(self.MEASURE_PREFIX)

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.swift.put_object(
            self.MEASURE_PREFIX,
            six.text_type(metric.id) + "/" + six.text_type(uuid.uuid4()) + now,
            data)

    def _build_report(self, details):
        metric_details = defaultdict(int)
        if details:
            headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                      full_listing=True)
            for f in files:
                metric, __ = f['name'].split("/", 1)
                metric_details[metric] += 1
            nb_metrics = len(metric_details)
        else:
            headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                      delimiter='/',
                                                      full_listing=True)
            nb_metrics = len(files)
        measures = int(headers.get('x-container-object-count'))
        return nb_metrics, measures, metric_details if details else None

    def list_metric_with_measures_to_process(self, size, part, full=False):
        limit = None
        if not full:
            limit = size * (part + 1)
        headers, files = self.swift.get_container(self.MEASURE_PREFIX,
                                                  delimiter='/',
                                                  full_listing=full,
                                                  limit=limit)
        if not full:
            files = files[size * part:]
        return set(f['subdir'][:-1] for f in files if 'subdir' in f)

    def _list_measure_files_for_metric_id(self, metric_id):
        headers, files = self.swift.get_container(
            self.MEASURE_PREFIX, path=six.text_type(metric_id),
            full_listing=True)
        return files

    def delete_unprocessed_measures_for_metric_id(self, metric_id):
        files = self._list_measure_files_for_metric_id(metric_id)
        swift.bulk_delete(self.swift, self.MEASURE_PREFIX, files)

    @contextlib.contextmanager
    def process_measure_for_metric(self, metric):
        files = self._list_measure_files_for_metric_id(metric.id)

        measures = []
        for f in files:
            headers, data = self.swift.get_object(
                self.MEASURE_PREFIX, f['name'])
            measures.extend(self._unserialize_measures(f['name'], data))

        yield measures

        # Now clean objects
        swift.bulk_delete(self.swift, self.MEASURE_PREFIX, files)
