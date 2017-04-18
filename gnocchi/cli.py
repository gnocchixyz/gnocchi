# Copyright (c) 2013 Mirantis Inc.
# Copyright (c) 2015-2016 Red Hat
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
import threading
import time

import cotyledon
from futurist import periodics
import msgpack
from oslo_config import cfg
from oslo_log import log
from oslo_utils import timeutils
import six
import tooz
from tooz import coordination

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage
from gnocchi import utils


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
            worker_id, conf, conf.storage.metric_reporting_delay)

    def _run_job(self):
        try:
            report = self.store.measures_report(details=False)
            LOG.info("%d measurements bundles across %d "
                     "metrics wait to be processed.",
                     report['summary']['measures'],
                     report['summary']['metrics'])
        except Exception:
            LOG.error("Unexpected error during pending measures reporting",
                      exc_info=True)


class MetricScheduler(MetricProcessBase):
    name = "scheduler"
    MAX_OVERLAP = 0.3
    GROUP_ID = "gnocchi-scheduler"
    SYNC_RATE = 30
    TASKS_PER_WORKER = 16
    BLOCK_SIZE = 4

    def __init__(self, worker_id, conf, queue):
        super(MetricScheduler, self).__init__(
            worker_id, conf, conf.storage.metric_processing_delay)
        self._coord, self._my_id = utils.get_coordinator_and_start(
            conf.storage.coordination_url)
        self.queue = queue
        self.previously_scheduled_metrics = set()
        self.workers = conf.metricd.workers
        self.block_index = 0
        self.block_size_default = self.workers * self.TASKS_PER_WORKER
        self.block_size = self.block_size_default
        self.block_synced = False
        self.periodic = None

    def set_block(self, event):
        self.block_synced = False
        get_members_req = self._coord.get_members(self.GROUP_ID)
        try:
            members = sorted(get_members_req.get())
            self.block_index = members.index(self._my_id)
            reqs = list(self._coord.get_member_capabilities(self.GROUP_ID, m)
                        for m in members)
            for req in reqs:
                cap = msgpack.loads(req.get(), encoding='utf-8')
                max_workers = max(cap['workers'], self.workers)
            self.block_size = max_workers * self.TASKS_PER_WORKER
            self.block_synced = True
            LOG.info('New set of agents detected. Now working on block: %s, '
                     'with up to %s metrics', self.block_index,
                     self.block_size)
        except Exception:
            LOG.error('Error getting block to work on (%s), '
                      'defaulting to first', exc_info=True)
            self.block_index = 0
            self.block_size = self.block_size_default

    @utils.retry
    def _configure(self):
        super(MetricScheduler, self)._configure()
        try:
            cap = msgpack.dumps({'workers': self.workers})
            join_req = self._coord.join_group(self.GROUP_ID, cap)
            join_req.get()
            LOG.info('Joined coordination group: %s', self.GROUP_ID)
            self.set_block(None)

            @periodics.periodic(spacing=self.SYNC_RATE, run_immediately=True)
            def run_watchers():
                done = self._coord.run_watchers()
                if not done and not self.block_synced:
                    self.set_block(None)

            self.periodic = periodics.PeriodicWorker.create([])
            self.periodic.add(run_watchers)
            t = threading.Thread(target=self.periodic.start)
            t.daemon = True
            t.start()

            self._coord.watch_join_group(self.GROUP_ID, self.set_block)
            self._coord.watch_leave_group(self.GROUP_ID, self.set_block)
        except coordination.GroupNotCreated as e:
            create_group_req = self._coord.create_group(self.GROUP_ID)
            try:
                create_group_req.get()
            except coordination.GroupAlreadyExist:
                pass
            raise utils.Retry(e)
        except tooz.NotImplemented:
            LOG.warning('Configured coordination driver does not support '
                        'required functionality. Coordination is disabled.')
        except Exception as e:
            LOG.error('Failed to configure coordination. Coordination is '
                      'disabled: %s', e)

    def _run_job(self):
        try:
            metrics = set(self.store.list_metric_with_measures_to_process(
                self.block_size, self.block_index))
            if metrics and not self.queue.empty():
                # NOTE(gordc): drop metrics we previously process to avoid
                #              handling twice
                number_of_scheduled_metrics = len(metrics)
                metrics = metrics - self.previously_scheduled_metrics
                if (float(number_of_scheduled_metrics - len(metrics)) /
                        self.block_size > self.MAX_OVERLAP):
                    LOG.warning('Metric processing lagging scheduling rate. '
                                'It is recommended to increase the number of '
                                'workers or to lengthen processing interval.')
            metrics = list(metrics)
            for i in six.moves.range(0, len(metrics), self.BLOCK_SIZE):
                self.queue.put(metrics[i:i + self.BLOCK_SIZE])
            self.previously_scheduled_metrics = set(metrics)
            LOG.debug("%d metrics scheduled for processing.", len(metrics))
        except Exception:
            LOG.error("Unexpected error scheduling metrics for processing",
                      exc_info=True)

    def close_services(self):
        if self.periodic:
            self.periodic.stop()
            self.periodic.wait()
        self._coord.leave_group(self.GROUP_ID)
        self._coord.stop()


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
        super(MetricProcessor, self).__init__(worker_id, conf, 0)
        self.queue = queue

    def _run_job(self):
        try:
            try:
                metrics = self.queue.get(block=True, timeout=10)
            except six.moves.queue.Empty:
                # NOTE(sileht): Allow the process to exit gracefully every
                # 10 seconds
                return
            self.store.process_background_tasks(self.index, metrics)
        except Exception:
            LOG.error("Unexpected error during measures processing",
                      exc_info=True)


class MetricdServiceManager(cotyledon.ServiceManager):
    def __init__(self, conf):
        super(MetricdServiceManager, self).__init__()
        self.conf = conf
        self.queue = multiprocessing.Manager().Queue()

        self.add(MetricScheduler, args=(self.conf, self.queue))
        self.add(MetricProcessor, args=(self.conf, self.queue),
                 workers=conf.metricd.workers)
        self.add(MetricReporting, args=(self.conf,))
        self.add(MetricJanitor, args=(self.conf,))

    def run(self):
        super(MetricdServiceManager, self).run()
        self.queue.close()


def metricd():
    conf = service.prepare_service()
    MetricdServiceManager(conf).run()
