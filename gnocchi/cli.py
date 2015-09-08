# Copyright (c) 2013 Mirantis Inc.
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
import logging
import multiprocessing
import signal
import sys
import time

from oslo_utils import timeutils
import retrying

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy as sql_db
from gnocchi.rest import app
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage


LOG = logging.getLogger(__name__)


def storage_dbsync():
    conf = service.prepare_service()
    indexer = sql_db.SQLAlchemyIndexer(conf)
    indexer.connect()
    indexer.upgrade()


def api():
    app.build_server()


def statsd():
    statsd_service.start()


class MetricProcessor(multiprocessing.Process):

    def __init__(self, conf, worker_number):
        super(MetricProcessor, self).__init__()
        self.conf = conf
        self.delay = worker_number

    # Retry with exponential backoff for up to 5 minutes
    @retrying.retry(wait_exponential_multiplier=500,
                    wait_exponential_max=60000,
                    stop_max_delay=300000)
    def _configure(self):
        self.store = storage.get_driver(self.conf)
        self.index = indexer.get_driver(self.conf)
        self.index.connect()

    def run(self):
        self._configure()
        # Delay startup so workers are jittered.
        time.sleep(self.delay)

        while True:
            try:
                with timeutils.StopWatch() as timer:
                    self._process(self.store, self.index)
                    time.sleep(
                        max(0, self.conf.storage.metric_processing_delay
                            - timer.elapsed()))
            except KeyboardInterrupt:
                # Ignore KeyboardInterrupt so parent handler can kill
                # all children.
                pass

    @staticmethod
    def _process(store, index):
        LOG.debug("Processing new measures")
        try:
            store.process_measures(index)
        except Exception:
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)


def metricd():
    conf = service.prepare_service()

    # Check that the storage driver actually needs this daemon to run
    s = storage.get_driver_class(conf)
    if s.process_measures == storage.StorageDriver.process_measures:
        LOG.debug("This storage driver does not need metricd to run, exiting")
        return 0

    signal.signal(signal.SIGTERM, _metricd_terminate)

    try:
        workers = []
        for worker in range(conf.metricd.workers):
            metric_worker = MetricProcessor(conf, worker)
            metric_worker.start()
            workers.append(metric_worker)

        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        _metricd_cleanup(workers)
        sys.exit(0)
    except Exception:
        LOG.warn("exiting", exc_info=True)
        _metricd_cleanup(workers)
        sys.exit(1)


def _metricd_cleanup(workers):
    for worker in workers:
        worker.terminate()
    for worker in workers:
        worker.join()


def _metricd_terminate(signum, frame):
    _metricd_cleanup(multiprocessing.active_children())
    sys.exit(0)
