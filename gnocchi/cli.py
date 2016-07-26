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
import multiprocessing
import sys
import threading
import time

import cotyledon
from oslo_config import cfg
from oslo_log import log
from oslo_utils import timeutils
import retrying
import six

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage


LOG = log.getLogger(__name__)


def upgrade():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.BoolOpt("skip-index", default=False,
                    help="Skip index upgrade."),
        cfg.BoolOpt("skip-storage", default=False,
                    help="Skip storage upgrade."),
        cfg.BoolOpt("skip-archive-policies-creation", default=False,
                    help="Skip default archive policies creation."),
        cfg.BoolOpt("create-legacy-resource-types", default=False,
                    help="Creation of Ceilometer legacy resource types.")
    ])
    conf = service.prepare_service(conf=conf)
    index = indexer.get_driver(conf)
    index.connect()
    if not conf.skip_index:
        LOG.info("Upgrading indexer %s" % index)
        index.upgrade(
            create_legacy_resource_types=conf.create_legacy_resource_types)
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s" % s)
        s.upgrade(index)

    if (not conf.skip_archive_policies_creation
            and not index.list_archive_policies()
            and not index.list_archive_policy_rules()):
        for name, ap in six.iteritems(archive_policy.DEFAULT_ARCHIVE_POLICIES):
            index.create_archive_policy(ap)
        index.create_archive_policy_rule("default", "*", "low")


def statsd():
    statsd_service.start()


class Retry(Exception):
    pass


def retry_if_retry_is_raised(exception):
    return isinstance(exception, Retry)


class MetricProcessBase(cotyledon.Service):
    def __init__(self, worker_id, conf, interval_delay=0):
        super(MetricProcessBase, self).__init__(worker_id)
        self.conf = conf
        self.startup_delay = worker_id
        self.interval_delay = interval_delay
        self._shutdown = threading.Event()
        self._shutdown_done = threading.Event()

    # Retry with exponential backoff for up to 1 minute
    @retrying.retry(wait_exponential_multiplier=500,
                    wait_exponential_max=60000,
                    retry_on_exception=retry_if_retry_is_raised)
    def _configure(self):
        try:
            self.store = storage.get_driver(self.conf)
            self.store.partition = self.worker_id
        except storage.StorageError as e:
            LOG.error("Unable to initialize storage: %s" % e)
            raise Retry(e)
        try:
            self.index = indexer.get_driver(self.conf)
            self.index.connect()
        except indexer.IndexerException as e:
            LOG.error("Unable to initialize indexer: %s" % e)
            raise Retry(e)

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
        self.close_queues()
        LOG.info("Waiting ongoing metric processing to finish")
        self._shutdown_done.wait()

    @staticmethod
    def close_queues():
        raise NotImplementedError

    @staticmethod
    def _run_job():
        raise NotImplementedError


class MetricReporting(MetricProcessBase):
    name = "reporting"

    def __init__(self, worker_id, conf, queues):
        super(MetricReporting, self).__init__(
            worker_id, conf, conf.storage.metric_reporting_delay)
        self.queues = queues

    def _run_job(self):
        try:
            report = self.store.measures_report(details=False)
            block_size = max(16, min(
                256, report['summary']['metrics'] // len(self.queues)))
            for queue in self.queues:
                queue.put(block_size)
            LOG.info("%d measurements bundles across %d "
                     "metrics wait to be processed.",
                     report['summary']['measures'],
                     report['summary']['metrics'])
        except Exception:
            LOG.error("Unexpected error during pending measures reporting",
                      exc_info=True)

    def close_queues(self):
        for queue in self.queues:
            queue.close()


class MetricJanitor(MetricProcessBase):
    name = "janitor"

    def __init__(self,  worker_id, conf):
        super(MetricJanitor, self).__init__(
            worker_id, conf, conf.storage.metric_cleanup_delay)

    def _run_job(self):
        try:
            self.store.expunge_metrics(self.index)
            LOG.debug("Metrics marked for deletion removed from backend")
        except Exception:
            LOG.error("Unexpected error during metric cleanup", exc_info=True)


class MetricProcessor(MetricProcessBase):
    name = "processing"

    def __init__(self, worker_id, conf, queue):
        super(MetricProcessor, self).__init__(
            worker_id, conf, conf.storage.metric_processing_delay)
        self.queue = queue
        self.block_size = 128

    def _run_job(self):
        try:
            while not self.queue.empty():
                self.block_size = self.queue.get()
                LOG.debug("Re-configuring worker to handle up to %s "
                          "metrics", self.block_size)
            self.store.process_background_tasks(self.index, self.block_size)
        except Exception:
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)

    def close_queues(self):
        self.queue.close()


class MetricdServiceManager(cotyledon.ServiceManager):
    def __init__(self, conf):
        super(MetricdServiceManager, self).__init__()
        self.conf = conf
        self.queues = [multiprocessing.Queue()
                       for i in range(conf.metricd.workers)]

        self.add(self.create_processor, workers=conf.metricd.workers)
        self.add(MetricReporting, args=(self.conf, self.queues))
        self.add(MetricJanitor, args=(self.conf,))

    def create_processor(self, worker_id):
        queue = self.queues[worker_id - 1]
        return MetricProcessor(worker_id, self.conf, queue)


def metricd():
    conf = service.prepare_service()
    if (conf.storage.metric_reporting_delay <
            conf.storage.metric_processing_delay):
        LOG.error("Metric reporting must run less frequently then processing")
        sys.exit(0)
    MetricdServiceManager(conf).run()
