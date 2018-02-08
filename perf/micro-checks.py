# -*- encoding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import daiquiri
import numpy

from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils

LOG = daiquiri.getLogger("gnocchi.benchmark")

NOW = numpy.datetime64(utils.utcnow())

CONF = service.prepare_service()

LOG.debug("Setting up storage")
STORAGE = storage.get_driver(CONF)

LOG.debug("Setting up indexer")
INDEXER = indexer.get_driver(CONF)

metrics = INDEXER.list_metrics(details=True)
LOG.info("Listing %d metrics", len(metrics))

AGGREGATION_METHOD = 'last'

# STORAGE = storage.get_driver(CONF)
for metric in metrics:
    aggregations = []
    for d in metric.archive_policy.definition:
        aggregations.append(metric.archive_policy.get_aggregation(
            AGGREGATION_METHOD, d.granularity))

    try:
        measures = STORAGE.get_measures(metric, aggregations)
        LOG.info("metric %s have %s measures for 'last'", metric.id,
                 len(measures[AGGREGATION_METHOD]))
    except storage.MetricDoesNotExist:
        pass
