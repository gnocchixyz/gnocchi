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

from gnocchi import exceptions


class ReportGenerationError(Exception):
    pass


# TODO(sileht): We inherit from this storage driver temporary
# until we moved out all incoming code from here.
class StorageDriver(object):

    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def upgrade(indexer):
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

    def measures_report(details=True):
        """Return a report of pending to process measures.

        Only useful for drivers that process measurements in background

        :return: {'summary': {'metrics': count, 'measures': count},
                  'details': {metric_id: pending_measures_count}}
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def list_metric_with_measures_to_process(size, part, full=False):
        raise NotImplementedError
