# -*- encoding: utf-8 -*-
#
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

from oslo.config import cfg
from stevedore import driver

from gnocchi import exceptions


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
]


Measure = collections.namedtuple('Measure', ['timestamp', 'value'])


class Metric(object):
    __slots__ = ['name', 'archive_policy']

    def __init__(self, name, archive_policy):
        self.name = name
        self.archive_policy = archive_policy

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.name == self.name

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)

    def __str__(self):
        return self.name

    def __hash__(self):
        return id(self)


class InvalidQuery(Exception):
    pass


class MetricDoesNotExist(Exception):
    """Error raised when this metric does not exist."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricDoesNotExist, self).__init__(
            "Metric %s does not exist" % metric)


class AggregationDoesNotExist(Exception):
    """Error raised when the aggregation method doesn't exists for a metric."""

    def __init__(self, metric, method):
        self.metric = metric
        self.method = method
        super(AggregationDoesNotExist, self).__init__(
            "Aggregation method '%s' for metric %s does not exist" %
            (method, metric))


class MetricAlreadyExists(Exception):
    """Error raised when this metric already exists."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricAlreadyExists, self).__init__(
            "Metric %s already exists" % metric)


class NoDeloreanAvailable(Exception):
    """Error raised when trying to insert a value that is too old."""

    def __init__(self, first_timestamp, bad_timestamp):
        self.first_timestamp = first_timestamp
        self.bad_timestamp = bad_timestamp
        super(NoDeloreanAvailable, self).__init__(
            "%s is before %s" % (bad_timestamp, first_timestamp))


class MetricUnaggregatable(Exception):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics, reason):
        self.metrics = metrics
        self.reason = reason
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s"
            % (" ,".join((m.name for m in metrics)), reason))


def _get_driver(name, conf):
    """Return the driver named name.

    :param name: The name of the driver.
    :param conf: The conf to pass to the driver.
    """
    d = driver.DriverManager('gnocchi.storage',
                             name).driver
    return d(conf)


def get_driver(conf):
    """Return the configured driver."""
    return _get_driver(conf.storage.driver,
                       conf.storage)


class StorageDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def create_metric(metric):
        """Create a metric.

        :param metric: The metric object.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def add_measures(metric, measures):
        """Add a measure to a metric.

        :param metric: The metric measured.
        :param measures: The actual measures.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def get_measures(metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_metric(metric):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_cross_metric_measures(metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  needed_overlap=None):
        """Get aggregated measures of multiple entities.

        :param entities: The entities measured to aggregate.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def search_value(metrics, predicate, from_timestamp=None,
                     to_timestamp=None,
                     aggregation='mean'):
        """Search for an aggregated value that realizes a predicate.

        :param metrics: The list of metrics to look into.
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError
