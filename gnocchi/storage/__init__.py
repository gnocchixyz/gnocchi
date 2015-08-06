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
from oslo_config import cfg
from oslo_utils import timeutils
from stevedore import driver

from gnocchi import exceptions


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
    cfg.IntOpt('metric_processing_delay',
               default=5,
               help="How many seconds to wait between "
               "new metric measure processing"),
]


class Measure(object):
    def __init__(self, timestamp, value):
        self.timestamp = timeutils.normalize_time(timestamp)
        self.value = value

    def __iter__(self):
        """Allow to transform measure to tuple."""
        yield self.timestamp
        yield self.value


class Metric(object):
    def __init__(self, id, archive_policy,
                 created_by_user_id=None,
                 created_by_project_id=None,
                 name=None,
                 resource_id=None):
        self.id = id
        self.archive_policy = archive_policy
        self.created_by_user_id = created_by_user_id
        self.created_by_project_id = created_by_project_id
        self.name = name
        self.resource_id = resource_id

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)

    def __str__(self):
        return str(self.id)

    def __eq__(self, other):
        return (isinstance(self, Metric)
                and self.id == other.id
                and self.archive_policy == other.archive_policy
                and self.created_by_user_id == other.created_by_user_id
                and self.created_by_project_id == other.created_by_project_id
                and self.name == other.name
                and self.resource_id == other.resource_id)

    __hash__ = object.__hash__


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


class MetricUnaggregatable(Exception):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics, reason):
        self.metrics = metrics
        self.reason = reason
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s"
            % (" ,".join((str(m.id) for m in metrics)), reason))


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
    def stop():
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
    def process_measures(indexer=None):
        """Process added measures in background.

        Some drivers might need to have a background task running that process
        the measures sent to metrics. This is used for that.
        """

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
    def search_value(metrics, query, from_timestamp=None,
                     to_timestamp=None,
                     aggregation='mean'):
        """Search for an aggregated value that realizes a predicate.

        :param metrics: The list of metrics to look into.
        :param query: The query being sent.
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise exceptions.NotImplementedError
