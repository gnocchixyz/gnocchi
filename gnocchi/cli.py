# Copyright (c) 2013 Mirantis Inc.
# Copyright (c) 2015-2017 Red Hat
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
import sys
import threading
import time

import cotyledon
from cotyledon import oslo_config_glue
from oslo_config import cfg
from oslo_log import log
from oslo_utils import timeutils
import six

from gnocchi import archive_policy
from gnocchi import genconfig
from gnocchi import indexer
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage
from gnocchi.storage import incoming
from gnocchi import utils


LOG = log.getLogger(__name__)


def config_generator():
    return genconfig.prehook(None, sys.argv[1:])


def upgrade():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.BoolOpt("skip-index", default=False,
                    help="Skip index upgrade."),
        cfg.BoolOpt("skip-storage", default=False,
                    help="Skip storage upgrade."),
        cfg.BoolOpt("skip-archive-policies-creation", default=False,
                    help="Skip default archive policies creation."),
    ])
    conf = service.prepare_service(conf=conf)
    index = indexer.get_driver(conf)
    index.connect()
    if not conf.skip_index:
        LOG.info("Upgrading indexer %s", index)
        index.upgrade()
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s", s)
        s.upgrade(index)

    if (not conf.skip_archive_policies_creation
            and not index.list_archive_policies()
            and not index.list_archive_policy_rules()):
        for name, ap in six.iteritems(archive_policy.DEFAULT_ARCHIVE_POLICIES):
            index.create_archive_policy(ap)
        index.create_archive_policy_rule("default", "*", "low")


def statsd():
    statsd_service.start()


class MetricProcessBase(cotyledon.Service):
    def __init__(self, worker_id, conf, interval_delay=0):
        super(MetricProcessBase, self).__init__(worker_id)
        self.conf = conf
        self.startup_delay = worker_id
        self.interval_delay = interval_delay
        self._shutdown = threading.Event()
        self._shutdown_done = threading.Event()

    def _configure(self):
        self.store = storage.get_driver(self.conf)
        self.index = indexer.get_driver(self.conf)
        self.index.connect()

    def run(self):
        self._configure()
        # Delay startup so workers are jittered.
        time.sleep(self.startup_delay)

        while not self._shutdown.is_set():
            with timeutils.StopWatch() as timer:
                self._run_job()
                self._shutdown.wait(max(0, self.interval_delay -
                                        timer.elapsed()))
        self._shutdown_done.set()

    def terminate(self):
        self._shutdown.set()
        self.close_services()
        LOG.info("Waiting ongoing metric processing to finish")
        self._shutdown_done.wait()

    @staticmethod
    def close_services():
        pass

    @staticmethod
    def _run_job():
        raise NotImplementedError


class MetricReporting(MetricProcessBase):
    name = "reporting"

    def __init__(self, worker_id, conf):
        super(MetricReporting, self).__init__(
            worker_id, conf, conf.metricd.metric_reporting_delay)

    def _run_job(self):
        try:
            report = self.store.incoming.measures_report(details=False)
            LOG.info("%d measurements bundles across %d "
                     "metrics wait to be processed.",
                     report['summary']['measures'],
                     report['summary']['metrics'])
        except incoming.ReportGenerationError:
            LOG.warning("Unable to compute backlog. Retrying at next "
                        "interval.")
        except Exception:
            LOG.error("Unexpected error during pending measures reporting",
                      exc_info=True)


class MetricProcessor(MetricProcessBase):
    name = "processing"

    def __init__(self, worker_id, conf):
        super(MetricProcessor, self).__init__(
            worker_id, conf, conf.metricd.metric_processing_delay)
        self._coord, self._my_id = utils.get_coordinator_and_start(
            conf.storage.coordination_url)

    def _run_job(self):
        try:
            metrics = list(
                self.store.incoming.list_metric_with_measures_to_process())
            LOG.debug("%d metrics scheduled for processing.", len(metrics))
            self.store.process_background_tasks(self.index, metrics)
        except Exception:
            LOG.error("Unexpected error scheduling metrics for processing",
                      exc_info=True)

    def close_services(self):
        self._coord.stop()


class MetricJanitor(MetricProcessBase):
    name = "janitor"

    def __init__(self,  worker_id, conf):
        super(MetricJanitor, self).__init__(
            worker_id, conf, conf.metricd.metric_cleanup_delay)

    def _run_job(self):
        try:
            self.store.expunge_metrics(self.index)
            LOG.debug("Metrics marked for deletion removed from backend")
        except Exception:
            LOG.error("Unexpected error during metric cleanup", exc_info=True)


class MetricdServiceManager(cotyledon.ServiceManager):
    def __init__(self, conf):
        super(MetricdServiceManager, self).__init__()
        oslo_config_glue.setup(self, conf)

        self.conf = conf
        self.metric_processor_id = self.add(
            MetricProcessor, args=(self.conf,),
            workers=conf.metricd.workers)
        if self.conf.metricd.metric_reporting_delay >= 0:
            self.add(MetricReporting, args=(self.conf,))
        self.add(MetricJanitor, args=(self.conf,))

        self.register_hooks(on_reload=self.on_reload)

    def on_reload(self):
        # NOTE(sileht): We do not implement reload() in Workers so all workers
        # will received SIGHUP and exit gracefully, then their will be
        # restarted with the new number of workers. This is important because
        # we use the number of worker to declare the capability in tooz and
        # to select the block of metrics to proceed.
        self.reconfigure(self.metric_processor_id,
                         workers=self.conf.metricd.workers)

    def run(self):
        super(MetricdServiceManager, self).run()
        self.queue.close()


def metricd_tester(conf):
    # NOTE(sileht): This method is designed to be profiled, we
    # want to avoid issues with profiler and os.fork(), that
    # why we don't use the MetricdServiceManager.
    index = indexer.get_driver(conf)
    index.connect()
    s = storage.get_driver(conf)
    metrics = s.incoming.list_metric_with_measures_to_process()[
        :conf.stop_after_processing_metrics]
    s.process_new_measures(index, metrics, True)


def metricd():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.IntOpt("stop-after-processing-metrics",
                   default=0,
                   min=0,
                   help="Number of metrics to process without workers, "
                   "for testing purpose"),
    ])
    conf = service.prepare_service(conf=conf)

    if conf.stop_after_processing_metrics:
        metricd_tester(conf)
    else:
        MetricdServiceManager(conf).run()
