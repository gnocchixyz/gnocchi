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
import operator

import daiquiri
from oslo_config import cfg
from stevedore import driver

from gnocchi import exceptions
from gnocchi import indexer


OPTS = [
    cfg.StrOpt('driver',
               default='file',
               help='Storage driver to use'),
]

LOG = daiquiri.getLogger(__name__)


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
                 creator=None,
                 name=None,
                 resource_id=None):
        self.id = id
        self.archive_policy = archive_policy
        self.creator = creator
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
                and self.creator == other.creator
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


class LockedMetric(StorageError):
    """Error raised when this metric is already being handled by another."""

    def __init__(self, metric):
        self.metric = metric
        super(LockedMetric, self).__init__("Metric %s is locked" % metric)


def get_driver_class(namespace, conf):
    """Return the storage driver class.

    :param conf: The conf to use to determine the driver.
    """
    return driver.DriverManager(namespace,
                                conf.driver).driver


def get_incoming_driver(conf):
    """Return configured incoming driver only

    :param conf: incoming configuration only (not global)
    """
    return get_driver_class('gnocchi.incoming', conf)(conf)


def get_driver(conf, coord=None):
    """Return the configured driver."""
    incoming = get_driver_class('gnocchi.incoming', conf.incoming)(
        conf.incoming)
    return get_driver_class('gnocchi.storage', conf.storage)(
        conf.storage, incoming, coord)


class StorageDriver(object):
    def __init__(self, conf, incoming, coord=None):
        self.incoming = incoming

    @staticmethod
    def stop():
        pass

    def upgrade(self, num_sacks):
        self.incoming.upgrade(num_sacks)

    def process_background_tasks(self, index, metrics, sync=False):
        """Process background tasks for this storage.

        This calls :func:`process_new_measures` to process new measures

        :param index: An indexer to be used for querying metrics
        :param metrics: The list of metrics waiting for processing
        :param sync: If True, then process everything synchronously and raise
                     on error
        :type sync: bool
        """
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
                index.expunge_metric(m.id)
            except (indexer.NoSuchMetric, LockedMetric):
                # It's possible another process deleted or is deleting the
                # metric, not a big deal
                pass
            except Exception:
                if sync:
                    raise
                LOG.error("Unable to expunge metric %s from storage", m,
                          exc_info=True)

    @staticmethod
    def process_new_measures(indexer, metrics, sync=False):
        """Process added measures in background.

        Some drivers might need to have a background task running that process
        the measures sent to metrics. This is used for that.
        """

    @staticmethod
    def get_measures(metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean', granularity=None, resample=None):
        """Get a measure to a metric.

        :param metric: The metric measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param granularity: The granularity to retrieve.
        :param resample: The granularity to resample to.
        """
        if aggregation not in metric.archive_policy.aggregation_methods:
            raise AggregationDoesNotExist(metric, aggregation)

    @staticmethod
    def delete_metric(metric, sync=False):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_cross_metric_measures(metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  reaggregation=None, resample=None,
                                  granularity=None, needed_overlap=None,
                                  fill=None):
        """Get aggregated measures of multiple entities.

        :param entities: The entities measured to aggregate.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param granularity: The granularity to retrieve.
        :param aggregation: The type of aggregation to retrieve.
        :param reaggregation: The type of aggregation to compute
                              on the retrieved measures.
        :param resample: The granularity to resample to.
        :param fill: The value to use to fill in missing data in series.
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
                     aggregation='mean',
                     granularity=None):
        """Search for an aggregated value that realizes a predicate.

        :param metrics: The list of metrics to look into.
        :param query: The query being sent.
        :param from_timestamp: The timestamp to get the measure from.
        :param to_timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        :param granularity: The granularity to retrieve.
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
