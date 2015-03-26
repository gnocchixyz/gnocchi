# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import multiprocessing
import operator
import random
import uuid

from concurrent import futures
from oslo_config import cfg
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage

OPTS = [
    cfg.IntOpt('aggregation_workers_number',
               default=None,
               help='Number of workers to run during adding new measures for '
                    'pre-aggregation needs.'),
    cfg.StrOpt('coordination_url',
               help='Coordination driver URL',
               default="file:///var/lib/gnocchi/locks"),

]


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
                raise storage.InvalidQuery("Unknown operator %s" % operator)
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
            raise storage.InvalidQuery(
                "Binary operator %s needs 2 arguments, %d given" %
                (op, len(nodes)))
        node0 = self.build_evaluator(node[0])
        node1 = self.build_evaluator(node[1])
        return lambda value: op(node0(value), node1(value))


class CarbonaraBasedStorageToozLock(object):
    def __init__(self, conf):
        self.coord = coordination.get_coordinator(
            conf.coordination_url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()

    def __del__(self):
        self.coord.stop()

    def __call__(self, metric, aggregation):
        lock_name = (b"gnocchi-" + str(metric.id).encode('ascii')
                     + b"-" + aggregation.encode('ascii'))
        return self.coord.get_lock(lock_name)


class CarbonaraBasedStorage(storage.StorageDriver):
    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self.executor = futures.ThreadPoolExecutor(
            max_workers=(conf.aggregation_workers_number or
                         multiprocessing.cpu_count()))

    @staticmethod
    def _create_metric_container(metric, archive_policy):
        pass

    @staticmethod
    def _lock(metric, aggregation):
        raise NotImplementedError

    def create_metric(self, metric):
        self._create_metric_container(metric)
        for aggregation in metric.archive_policy.aggregation_methods:
            # TODO(jd) Having the TimeSerieArchive.full_res_timeserie duped in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            archive = carbonara.TimeSerieArchive.from_definitions(
                [(v.granularity, v.points)
                 for v in metric.archive_policy.definition],
                back_window=metric.archive_policy.back_window,
                aggregation_method=aggregation)
            self._store_metric_measures(metric, aggregation,
                                        archive.serialize())

    @staticmethod
    def _get_measures(metric, aggregation):
        raise NotImplementedError

    @staticmethod
    def _store_metric_measures(metric, aggregation, data):
        raise NotImplementedError

    def get_measures(self, metric, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        archive = self._get_measures_archive(metric, aggregation)
        return archive.fetch(from_timestamp, to_timestamp)

    def _get_measures_archive(self, metric, aggregation):
        contents = self._get_measures(metric, aggregation)
        return carbonara.TimeSerieArchive.unserialize(contents)

    def _add_measures(self, aggregation, metric, measures):
        with self._lock(metric, aggregation):
            contents = self._get_measures(metric, aggregation)
            archive = carbonara.TimeSerieArchive.unserialize(contents)
            try:
                archive.set_values([(m.timestamp, m.value)
                                    for m in measures])
            except carbonara.NoDeloreanAvailable as e:
                raise storage.NoDeloreanAvailable(e.first_timestamp,
                                                  e.bad_timestamp)
            self._store_metric_measures(metric, aggregation,
                                        archive.serialize())

    def add_measures(self, metric, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        # NOTE(jd) So this is a (smart?) optimization: since we're going to
        # lock for each of this aggregation type, if we are using running
        # Gnocchi with multiple processes, let's randomize what we iter
        # over so there are less chances we fight for the same lock!
        agg_methods = list(metric.archive_policy.aggregation_methods)
        random.shuffle(agg_methods)
        self._map_in_thread(self._add_measures,
                            list((aggregation, metric, measures)
                                 for aggregation
                                 in agg_methods))

    def get_cross_metric_measures(self, metrics, from_timestamp=None,
                                  to_timestamp=None, aggregation='mean',
                                  needed_overlap=100.0):

        tss = self._map_in_thread(self._get_measures_archive,
                                  [(metric, aggregation)
                                   for metric in metrics])
        try:
            return carbonara.TimeSerieArchive.aggregated(
                tss, from_timestamp, to_timestamp, aggregation, needed_overlap)
        except carbonara.UnAggregableTimeseries as e:
            raise storage.MetricUnaggregatable(metrics, e.reason)

    def _find_measure(self, metric, aggregation, predicate,
                      from_timestamp, to_timestamp):
        timeserie = self._get_measures_archive(metric, aggregation)
        values = timeserie.fetch(from_timestamp, to_timestamp)
        return {metric:
                [(timestamp, granularity, value)
                 for timestamp, granularity, value in values
                 if predicate(value)]}

    def search_value(self, metrics, query, from_timestamp=None,
                     to_timestamp=None, aggregation='mean'):
        result = {}
        predicate = MeasureQuery(query)
        results = self._map_in_thread(self._find_measure,
                                      [(metric, aggregation, predicate,
                                        from_timestamp, to_timestamp)
                                       for metric in metrics])
        for r in results:
            result.update(r)
        return result

    def _map_in_thread(self, method, list_of_args):
        # We use 'list' to iterate all threads here to raise the first
        # exception now , not much choice
        return list(self.executor.map(lambda args: method(*args),
                                      list_of_args))
