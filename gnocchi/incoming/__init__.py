# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
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
import collections

import daiquiri
import numpy
import six

from gnocchi.carbonara import TIMESERIES_ARRAY_DTYPE
from gnocchi import exceptions
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


Measure = collections.namedtuple("Measure", ['timestamp', 'value'])


class ReportGenerationError(Exception):
    pass


class SackDetectionError(Exception):
    pass


class IncomingDriver(object):
    MEASURE_PREFIX = "measure"
    SACK_PREFIX = "incoming"
    CFG_PREFIX = 'gnocchi-config'
    CFG_SACKS = 'sacks'

    @property
    def NUM_SACKS(self):
        if not hasattr(self, '_num_sacks'):
            try:
                self._num_sacks = int(self._get_storage_sacks())
            except Exception as e:
                raise SackDetectionError(e)
        return self._num_sacks

    @staticmethod
    def __init__(conf, greedy=True):
        pass

    def get_sack_prefix(self, num_sacks=None):
        sacks = num_sacks if num_sacks else self.NUM_SACKS
        return self.SACK_PREFIX + str(sacks) + '-%s'

    def upgrade(self, num_sacks):
        try:
            self.NUM_SACKS
        except SackDetectionError:
            self.set_storage_settings(num_sacks)

    @staticmethod
    def set_storage_settings(num_sacks):
        raise exceptions.NotImplementedError

    @staticmethod
    def remove_sack_group(num_sacks):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_storage_sacks():
        """Return the number of sacks in storage. None if not set."""
        raise exceptions.NotImplementedError

    @staticmethod
    def get_sack_lock(coord, sack):
        lock_name = b'gnocchi-sack-%s-lock' % str(sack).encode('ascii')
        return coord.get_lock(lock_name)

    def _make_measures_array(self):
        return numpy.array([], dtype=TIMESERIES_ARRAY_DTYPE)

    @staticmethod
    def _array_concatenate(arrays):
        if arrays:
            return numpy.concatenate(arrays)
        return arrays

    def _unserialize_measures(self, measure_id, data):
        try:
            return numpy.frombuffer(data, dtype=TIMESERIES_ARRAY_DTYPE)
        except ValueError:
            LOG.error(
                "Unable to decode measure %s, possible data corruption",
                measure_id)
            raise

    def _encode_measures(self, measures):
        return numpy.array(list(measures),
                           dtype=TIMESERIES_ARRAY_DTYPE).tobytes()

    def add_measures(self, metric_id, measures):
        """Add a measure to a metric.

        :param metric_id: The metric measured.
        :param measures: The actual measures.
        """
        self.add_measures_batch({metric_id: measures})

    def add_measures_batch(self, metrics_and_measures):
        """Add a batch of measures for some metrics.

        :param metrics_and_measures: A dict where keys are metric objects
                                     and values are a list of
                                     :py:class:`gnocchi.incoming.Measure`.
        """
        utils.parallel_map(
            self._store_new_measures,
            ((metric_id, self._encode_measures(measures))
             for metric_id, measures
             in six.iteritems(metrics_and_measures)))

    @staticmethod
    def _store_new_measures(metric_id, data):
        raise exceptions.NotImplementedError

    def measures_report(self, details=True):
        """Return a report of pending to process measures.

        Only useful for drivers that process measurements in background

        :return: {'summary': {'metrics': count, 'measures': count},
                  'details': {metric_id: pending_measures_count}}
        """
        metrics, measures, full_details = self._build_report(details)
        report = {'summary': {'metrics': metrics, 'measures': measures}}
        if full_details is not None:
            report['details'] = full_details
        return report

    @staticmethod
    def _build_report(details):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_metric_with_measures_to_process(sack):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_unprocessed_measures_for_metric(metric_id):
        raise exceptions.NotImplementedError

    @staticmethod
    def process_measure_for_metric(metric_id):
        raise exceptions.NotImplementedError

    @staticmethod
    def has_unprocessed(metric_id):
        raise exceptions.NotImplementedError

    def sack_for_metric(self, metric_id):
        return metric_id.int % self.NUM_SACKS

    def get_sack_name(self, sack):
        return self.get_sack_prefix() % sack

    @staticmethod
    def iter_on_sacks_to_process():
        """Return an iterable of sack that got new measures to process."""
        raise exceptions.NotImplementedError

    @staticmethod
    def finish_sack_processing(sack):
        """Mark sack processing has finished."""
        pass


@utils.retry_on_exception_and_log("Unable to initialize incoming driver")
def get_driver(conf):
    """Return configured incoming driver only

    :param conf: incoming configuration only (not global)
    """
    return utils.get_driver_class('gnocchi.incoming', conf.incoming)(
        conf.incoming, conf.metricd.greedy)
