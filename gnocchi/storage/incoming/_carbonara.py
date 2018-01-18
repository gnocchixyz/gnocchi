# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
from concurrent import futures
import itertools
import operator
import struct

from oslo_log import log
from oslo_serialization import msgpackutils
import pandas
import six

from gnocchi.storage import incoming
from gnocchi import utils

LOG = log.getLogger(__name__)

_NUM_WORKERS = utils.get_default_workers()


class CarbonaraBasedStorage(incoming.StorageDriver):
    MEASURE_PREFIX = "measure"
    _MEASURE_SERIAL_FORMAT = "Qd"
    _MEASURE_SERIAL_LEN = struct.calcsize(_MEASURE_SERIAL_FORMAT)

    def _unserialize_measures(self, measure_id, data):
        nb_measures = len(data) // self._MEASURE_SERIAL_LEN
        try:
            measures = struct.unpack(
                "<" + self._MEASURE_SERIAL_FORMAT * nb_measures, data)
        except struct.error:
            # This either a corruption, either a v2 measures
            try:
                return msgpackutils.loads(data)
            except ValueError:
                LOG.error(
                    "Unable to decode measure %s, possible data corruption",
                    measure_id)
                raise
        return six.moves.zip(
            pandas.to_datetime(measures[::2], unit='ns'),
            itertools.islice(measures, 1, len(measures), 2))

    def _encode_measures(self, measures):
        measures = list(measures)
        return struct.pack(
            "<" + self._MEASURE_SERIAL_FORMAT * len(measures),
            *list(itertools.chain.from_iterable(measures)))

    def add_measures_batch(self, metrics_and_measures):
        with futures.ThreadPoolExecutor(max_workers=_NUM_WORKERS) as executor:
            list(executor.map(
                lambda args: self._store_new_measures(*args),
                ((metric, self._encode_measures(measures))
                 for metric, measures
                 in six.iteritems(metrics_and_measures))))

    @staticmethod
    def _store_new_measures(metric, data):
        raise NotImplementedError

    def measures_report(self, details=True):
        metrics, measures, full_details = self._build_report(details)
        report = {'summary': {'metrics': metrics, 'measures': measures}}
        if full_details is not None:
            report['details'] = full_details
        return report

    @staticmethod
    def _build_report(details):
        raise NotImplementedError

    def list_metric_with_measures_to_process(self, size, part, full=False):
        metrics = map(operator.itemgetter(0),
                      # Sort by the number of measures, bigger first (reverse)
                      sorted(
                          self._list_metric_with_measures_to_process(),
                          key=operator.itemgetter(1),
                          reverse=True))
        if full:
            return set(metrics)
        return set(list(metrics)[size * part:size * (part + 1)])

    @staticmethod
    def _list_metric_with_measures_to_process():
        """Return an ordered list of metrics that needs to be processed."""
        raise NotImplementedError

    @staticmethod
    def delete_unprocessed_measures_for_metric_id(metric_id):
        raise NotImplementedError

    @staticmethod
    def process_measure_for_metric(metric):
        raise NotImplementedError
