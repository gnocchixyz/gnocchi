# -*- encoding: utf-8 -*-
#
# Copyright © 2017-2018 Red Hat, Inc.
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
import functools
import itertools
import operator

import daiquiri
import numpy
import six

from gnocchi.carbonara import TIMESERIES_ARRAY_DTYPE
from gnocchi import exceptions
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


Measure = collections.namedtuple("Measure", ['timestamp', 'value'])


ITEMGETTER_1 = operator.itemgetter(1)


class ReportGenerationError(Exception):
    pass


class SackDetectionError(Exception):
    pass


@functools.total_ordering
class Sack(object):
    """A sack is a recipient that contains measures for a group of metrics.

    It is identified by a positive integer called `number`.
    """

    # Use slots to make them as small as possible since we can create a ton of
    # those.
    __slots__ = [
        "number",
        "total",
        "name",
    ]

    def __init__(self, number, total, name):
        """Create a new sack.

        :param number: The sack number, identifying it.
        :param total: The total number of sacks.
        :param name: The sack name.
        """
        self.number = number
        self.total = total
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%s(%d/%d) %s>" % (
            self.__class__.__name__, self.number, self.total, str(self),
        )

    def _compare(self, op, other):
        if isinstance(other, Sack):
            if self.total != other.total:
                raise TypeError(
                    "Cannot compare %s with different total number" %
                    self.__class__.__name__)
            return op(self.number, other.number)
        raise TypeError("Cannot compare %r with %r" % (self, other))

    def __lt__(self, other):
        return self._compare(operator.lt, other)

    def __eq__(self, other):
        return self._compare(operator.eq, other)

    def __ne__(self, other):
        # neither total_ordering nor py2 sets ne as the opposite of eq
        return self._compare(operator.ne, other)

    def __hash__(self):
        return hash(self.name)


class IncomingDriver(object):
    MEASURE_PREFIX = "measure"
    SACK_NAME_FORMAT = "incoming{total}-{number}"
    CFG_PREFIX = 'gnocchi-config'
    CFG_SACKS = 'sacks'
    # NOTE(sileht): By default we use threads, but some driver can disable
    # threads by setting this to utils.sequencial_map
    MAP_METHOD = staticmethod(utils.parallel_map)

    @property
    def NUM_SACKS(self):
        if not hasattr(self, '_num_sacks'):
            try:
                self._num_sacks = int(self._get_storage_sacks())
            except Exception as e:
                raise SackDetectionError(e)
        return self._num_sacks

    def __init__(self, conf, greedy=True):
        self._sacks = None

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
        return numpy.fromiter(measures,
                              dtype=TIMESERIES_ARRAY_DTYPE).tobytes()

    def group_metrics_by_sack(self, metrics):
        """Iterate on a list of metrics, grouping them by sack.

        :param metrics: A list of metric uuid.
        :return: An iterator yield (group, metrics).
        """
        metrics_and_sacks = sorted(
            ((m, self.sack_for_metric(m)) for m in metrics),
            key=ITEMGETTER_1)
        for sack, metrics in itertools.groupby(metrics_and_sacks,
                                               key=ITEMGETTER_1):
            yield sack, [m[0] for m in metrics]

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
        self.MAP_METHOD(self._store_new_measures,
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
    def delete_unprocessed_measures_for_metric(metric_id):
        raise exceptions.NotImplementedError

    @staticmethod
    def process_measure_for_metrics(metric_id):
        raise exceptions.NotImplementedError

    @staticmethod
    def process_measures_for_sack(sack):
        raise exceptions.NotImplementedError

    @staticmethod
    def has_unprocessed(metric_id):
        raise exceptions.NotImplementedError

    def _get_sack_name(self, number):
        return self.SACK_NAME_FORMAT.format(
            total=self.NUM_SACKS, number=number)

    def _make_sack(self, i):
        return Sack(i, self.NUM_SACKS, self._get_sack_name(i))

    def sack_for_metric(self, metric_id):
        return self._make_sack(metric_id.int % self.NUM_SACKS)

    def iter_sacks(self):
        return (self._make_sack(i) for i in six.moves.range(self.NUM_SACKS))

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
