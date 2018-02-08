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


import cProfile
import multiprocessing.queues
import os
import random
import uuid

import daiquiri
import numpy

import gc

from gnocchi import chef
from gnocchi.cli import metricd
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils

LOG = daiquiri.getLogger("gnocchi.benchmark")

NOW = numpy.datetime64(utils.utcnow())

MEASURES_PER_METRIC = 60        # seconds of data
_NODES = int(os.getenv("MICRO_METRICD_NODES", "1"))
_VM_PER_NODES = 10
METRICS = _NODES * 100 + _VM_PER_NODES * _NODES * 30
# 1000 nodes * 100 metrics + 10 vm per node * 30 metrics per vm

ARCHIVE_POLICY_NAME = "bool"

CONF = service.prepare_service()

if CONF.storage.driver == "rocksdb222":
    LOG.debug("Setting up rockdb writer")

    class Worker(multiprocessing.Process):
        def __init__(self, conf):
            from gnocchi.storage import rocksdb
            self._w = rocksdb.WriterService(conf)
            super(Worker, self).__init__()

        def run(self):
            self._w.run()

    rocksdb_worker = Worker(CONF)
    rocksdb_worker.start()

LOG.debug("Setting up incoming")
INCOMING = incoming.get_driver(CONF)

LOG.debug("Setting up coordinator")
COORD = metricd.get_coordinator_and_start(str(uuid.uuid4()),
                                          CONF.coordination_url)

LOG.debug("Setting up storage")
STORAGE = storage.get_driver(CONF)

LOG.debug("Setting up indexer")
INDEXER = indexer.get_driver(CONF)

LOG.debug("Setting up chef")
CHEF = chef.Chef(COORD, INCOMING, INDEXER, STORAGE)

LOG.info("Creating %d metrics", METRICS)
sw = utils.StopWatch().start()
metrics = [
    INDEXER.create_metric(uuid.uuid4(), "admin", ARCHIVE_POLICY_NAME).id
    for _ in range(METRICS)
]
LOG.info("Created %d metrics in %.2fs", METRICS, sw.elapsed())


LOG.info("Generating %d measures per metric for %d metrics… ",
         MEASURES_PER_METRIC, METRICS)
sw.reset()
measures = {
    m_id: [incoming.Measure(
        NOW + numpy.timedelta64(seconds=s),
        random.randint(-999999, 999999)) for s in range(MEASURES_PER_METRIC)]
    for m_id in metrics
}
LOG.info("… done in %.2fs", sw.elapsed())

sw.reset()
INCOMING.add_measures_batch(measures)
total_measures = sum(map(len, measures.values()))
LOG.info("Pushed %d measures in %.2fs",
         total_measures,
         sw.elapsed())

del measures
gc.collect()

sw.reset()

# True to enable profiling
PROF = None

if PROF:
    PROF = cProfile.Profile()
    PROF.enable()

# for s in INCOMING.iter_on_sacks_to_process():
for s in INCOMING.iter_sacks():
    LOG.debug("Getting lock for sack %d", s)
    CHEF.process_new_measures_for_sack(s)
    INCOMING.finish_sack_processing(s)
    LOG.debug("Processed sack %d", s)

if PROF:
    PROF.disable()
    # PROF.dump_stats("%s.cprof" % __file__)
    PROF.print_stats()

# if CONF.storage.driver == "rocksdb":
#     STORAGE.stop()

end = sw.elapsed()
if CONF.storage.driver == "rocksdb222":
    rocksdb_worker.terminate()

LOG.info("Processed %d sacks in %.2fs", INCOMING.NUM_SACKS, end)
LOG.info("Speed: %.2f measures/s", float(total_measures) / end)
