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
from gnocchi import storage


class NullStorage(storage.StorageDriver):

    def __init__(self, conf):
        self.metrics = {}

    def create_metric(self, metric):
        if metric.id in self.metrics:
            raise storage.MetricAlreadyExists(metric)
        self.metrics[metric.id] = True

    def delete_metric(self, metric):
        try:
            del self.metrics[metric.id]
        except KeyError:
            raise storage.MetricDoesNotExist(metric)
