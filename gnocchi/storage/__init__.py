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
import operator
from oslo_config import cfg
from oslo_log import log
from stevedore import driver

from gnocchi import exceptions
from gnocchi import indexer


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
    cfg.IntOpt('metric_processing_delay',
               default=60,
               help="How many seconds to wait between "
               "scheduling new metrics to process"),
    cfg.IntOpt('metric_reporting_delay',
               default=120,
               help="How many seconds to wait between "
               "metric ingestion reporting"),
    cfg.IntOpt('metric_cleanup_delay',
               default=300,
               help="How many seconds to wait between "
               "cleaning of expired data"),
]

LOG = log.getLogger(__name__)


class Measure(object):
    def __init__(self, timestamp, value):
        self.timestamp = timestamp
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
        return (isinstance(other, Metric)
                and self.id == other.id
                and self.archive_policy == other.archive_policy
                and self.created_by_user_id == other.created_by_user_id
                and self.created_by_project_id == other.created_by_project_id
                and self.name == other.name
                and self.resource_id == other.resource_id)

    __hash__ = object.__hash__


class StorageError(Exception):
    pass


class InvalidQuery(StorageError):
    pass


class MetricDoesNotExist(StorageError):
    """Error raised when this metric does not exist."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricDoesNotExist, self).__init__(
            "Metric %s does not exist" % metric)


class AggregationDoesNotExist(StorageError):
    """Error raised when the aggregation method doesn't exists for a metric."""

    def __init__(self, metric, method):
        self.metric = metric
        self.method = method
        super(AggregationDoesNotExist, self).__init__(
            "Aggregation method '%s' for metric %s does not exist" %
            (method, metric))


class GranularityDoesNotExist(StorageError):
    """Error raised when the granularity doesn't exist for a metric."""

    def __init__(self, metric, granularity):
        self.metric = metric
        self.granularity = granularity
        super(GranularityDoesNotExist, self).__init__(
            "Granularity '%s' for metric %s does not exist" %
            (granularity, metric))


class MetricAlreadyExists(StorageError):
    """Error raised when this metric already exists."""

    def __init__(self, metric):
        self.metric = metric
        super(MetricAlreadyExists, self).__init__(
            "Metric %s already exists" % metric)


class MetricUnaggregatable(StorageError):
    """Error raised when metrics can't be aggregated."""

    def __init__(self, metrics, reason):
        self.metrics = metrics
        self.reason = reason
        super(MetricUnaggregatable, self).__init__(
            "Metrics %s can't be aggregated: %s"
            % (", ".join((str(m.id) for m in metrics)), reason))


def get_driver_class(conf):
    """Return the storage driver class.

    :param conf: The conf to use to determine the driver.
    """
    return driver.DriverManager('gnocchi.storage',
                                conf.storage.driver).driver


def get_driver(conf):
    """Return the configured driver."""
    return get_driver_class(conf)(conf.storage)


class StorageDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def stop():
        pass

    @staticmethod
    def upgrade(index):
        pass

    def process_background_tasks(self, index, metrics, sync=False):
        """Process background tasks for this storage.

        This calls :func:`process_new_measures` to process new measures

        :param index: An indexer to be used for querying metrics
        :param block_size: number of metrics to process
        :param sync: If True, then process everything synchronously and raise
                     on error
        :type sync: bool
        """
        LOG.debug("Processing new measures")
        try:
            self.process_new_measures(index, metrics, sync)
        except Exception:
            if sync:
                raise
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)

    def expunge_metrics(self, index, sync=False):
        """Remove deleted metrics

        :param index: An indexer to be used for querying metrics
        :param sync: If True, then delete everything synchronously and raise
                     on error
        :type sync: bool
        """

        metrics_to_expunge = index.list_metrics(status='delete')
        for m in metrics_to_expunge:
            try:
                self.delete_metric(m, sync)
            except Exception:
                if sync:
                    raise
                LOG.error("Unable to expunge metric %s from storage" % m,
                          exc_info=True)
                continue
            try:
                index.expunge_metric(m.id)
            except indexer.NoSuchMetric:
                # It's possible another process deleted the metric in the mean
                # time, not a big deal
                pass

    def add_measures(self, metric, measures):
        """Add a measure to a metric.

        :param metric: The metric measured.
        :param measures: The actual measures.
        """
        self.add_measures_batch({metric: measures})

    @staticmethod
    def add_measures_batch(metrics_and_measures):
        """Add a batch of measures for some metrics.

        :param metrics_and_measures: A dict where keys
        are metrics and value are measure.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def process_new_measures(indexer, metrics, sync=False):
        """Process added measures in background.

        Some drivers might need to have a background task running that process
        the measures sent to metrics. This is used for that.
        """

    @staticmethod
    def measures_report(details=True):
        """Return a report of pending to process measures.

        Only useful for drivers that process measurements in background

        :return: {'summary': {'metrics': count, 'measures': count},
                  'details': {metric_id: pending_measures_count}}
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def get_measures(metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param granularity: The granularity to retrieve.
        """
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise AggregationDoesNotExist(metric, aggregation)

    @staticmethod
    def delete_metric(metric, sync=False):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_cross_metric_measures(metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None,
                                  granularity=None,
                                  needed_overlap=None):
        """Get aggregated measures of multiple entities.

        :param entities: The entities measured to aggregate.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param granularity: The granularity to retrieve.
        :param aggregation: The type of aggregation to retrieve.
        :param reaggregation: The type of aggregation to compute
                              on the retrieved measures.
        """
        for metric in metrics:
            if aggregation not in metric.archive_policy.aggregation_methods:
                raise AggregationDoesNotExist(metric, aggregation)
            if (granularity is not None and granularity
               not in set(d.granularity
                          for d in metric.archive_policy.definition)):
                raise GranularityDoesNotExist(metric, granularity)

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


class MeasureQuery(object):
    binary_operators = {
        u"=": operator.eq,
        u"==": operator.eq,
        u"eq": operator.eq,

        u"<": operator.lt,
        u"lt": operator.lt,

        u">": operator.gt,
        u"gt": operator.gt,

        u"<=": operator.le,
        u"≤": operator.le,
        u"le": operator.le,

        u">=": operator.ge,
        u"≥": operator.ge,
        u"ge": operator.ge,

        u"!=": operator.ne,
        u"≠": operator.ne,
        u"ne": operator.ne,

        u"%": operator.mod,
        u"mod": operator.mod,

        u"+": operator.add,
        u"add": operator.add,

        u"-": operator.sub,
        u"sub": operator.sub,

        u"*": operator.mul,
        u"×": operator.mul,
        u"mul": operator.mul,

        u"/": operator.truediv,
        u"÷": operator.truediv,
        u"div": operator.truediv,

        u"**": operator.pow,
        u"^": operator.pow,
        u"pow": operator.pow,
    }

    multiple_operators = {
        u"or": any,
        u"∨": any,
        u"and": all,
        u"∧": all,
    }

    def __init__(self, tree):
        self._eval = self.build_evaluator(tree)

    def __call__(self, value):
        return self._eval(value)

    def build_evaluator(self, tree):
        try:
            operator, nodes = list(tree.items())[0]
        except Exception:
            return lambda value: tree
        try:
            op = self.multiple_operators[operator]
        except KeyError:
            try:
                op = self.binary_operators[operator]
            except KeyError:
                raise InvalidQuery("Unknown operator %s" % operator)
            return self._handle_binary_op(op, nodes)
        return self._handle_multiple_op(op, nodes)

    def _handle_multiple_op(self, op, nodes):
        elements = [self.build_evaluator(node) for node in nodes]
        return lambda value: op((e(value) for e in elements))

    def _handle_binary_op(self, op, node):
        try:
            iterator = iter(node)
        except Exception:
            return lambda value: op(value, node)
        nodes = list(iterator)
        if len(nodes) != 2:
            raise InvalidQuery(
                "Binary operator %s needs 2 arguments, %d given" %
                (op, len(nodes)))
        node0 = self.build_evaluator(node[0])
        node1 = self.build_evaluator(node[1])
        return lambda value: op(node0(value), node1(value))
