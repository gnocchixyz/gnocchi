# Copyright (c) 2013 Mirantis Inc.
# Copyright (c) 2015 Red Hat
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

from oslo_config import cfg
from oslo_utils import timeutils
import retrying

from gnocchi import indexer
from gnocchi.rest import app
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage


LOG = logging.getLogger(__name__)


def upgrade():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.BoolOpt("skip-index", default=False,
                    help="Skip index upgrade."),
        cfg.BoolOpt("skip-storage", default=False,
                    help="Skip storage upgrade.")
    ])
    conf = service.prepare_service(conf=conf)
    if not conf.skip_index:
        index = indexer.get_driver(conf)
        index.connect()
        LOG.info("Upgrading indexer %s" % index)
        index.upgrade()
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s" % s)
        s.upgrade(index)


def api():
    app.build_server()


def statsd():
    statsd_service.start()


class MetricProcessBase(multiprocessing.Process):
    def __init__(self, conf, startup_delay=0, interval_delay=0):
        super(MetricProcessBase, self).__init__()
        self.conf = conf
        self.startup_delay = startup_delay
        self.interval_delay = interval_delay

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
        time.sleep(self.startup_delay)

        while True:
            try:
                with timeutils.StopWatch() as timer:
                    self._run_job()
                    time.sleep(max(0, self.interval_delay - timer.elapsed()))
            except KeyboardInterrupt:
                # Ignore KeyboardInterrupt so parent handler can kill
                # all children.
                pass

    @staticmethod
    def _run_job():
        raise NotImplementedError


class MetricReporting(MetricProcessBase):
    def _run_job(self):
        try:
            report = self.store.measures_report(details=False)
            LOG.info("Metricd reporting: %d measurements bundles across %d "
                     "metrics wait to be processed.",
                     report['summary']['measures'],
                     report['summary']['metrics'])
        except Exception:
            LOG.error("Unexpected error during pending measures reporting",
                      exc_info=True)


class MetricProcessor(MetricProcessBase):
    def _run_job(self):
        try:
            self.store.process_background_tasks(self.index)
        except Exception:
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)


def metricd():
    conf = service.prepare_service()

    signal.signal(signal.SIGTERM, _metricd_terminate)

    try:
        metric_report = MetricReporting(
            conf, 0, conf.storage.metric_reporting_delay)
        metric_report.start()

        workers = [metric_report]
        for worker in range(conf.metricd.workers):
            metric_worker = MetricProcessor(
                conf, worker, conf.storage.metric_processing_delay)
            metric_worker.start()
            workers.append(metric_worker)

        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        _metricd_cleanup(workers)
        sys.exit(0)
    except Exception:
        LOG.warning("exiting", exc_info=True)
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
