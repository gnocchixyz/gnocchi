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
import daiquiri
from futurist import periodics
from oslo_config import cfg
import six
import tenacity
import tooz

from gnocchi import archive_policy
from gnocchi import genconfig
from gnocchi import indexer
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage
from gnocchi.storage import incoming
from gnocchi import utils


LOG = daiquiri.getLogger(__name__)


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
        cfg.IntOpt("sacks-number", default=128, min=1,
                   help="Number of storage sacks to create."),

    ])
    conf = service.prepare_service(conf=conf)
    if not conf.skip_index:
        index = indexer.get_driver(conf)
        index.connect()
        LOG.info("Upgrading indexer %s", index)
        index.upgrade()
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s", s)
        s.upgrade(conf.sacks_number)

    if (not conf.skip_archive_policies_creation
            and not index.list_archive_policies()
            and not index.list_archive_policy_rules()):
        if conf.skip_index:
            index = indexer.get_driver(conf)
            index.connect()
        for name, ap in six.iteritems(archive_policy.DEFAULT_ARCHIVE_POLICIES):
            index.create_archive_policy(ap)
        index.create_archive_policy_rule("default", "*", "low")


def change_sack_size():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.IntOpt("sacks-number", required=True, min=1,
                   help="Number of storage sacks."),
    ])
    conf = service.prepare_service(conf=conf)
    s = storage.get_incoming_driver(conf.incoming)
    try:
        report = s.measures_report(details=False)
    except incoming.SackDetectionError:
        # issue is already logged by NUM_SACKS, abort.
        return
    remainder = report['summary']['measures']
    if remainder:
        LOG.error('Cannot change sack when non-empty backlog. Process '
                  'remaining %s measures and try again', remainder)
        return
    LOG.info("Changing sack size to: %s", conf.sacks_number)
    old_num_sacks = s.get_storage_sacks()
    s.set_storage_settings(conf.sacks_number)
    s.remove_sack_group(old_num_sacks)


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
            with utils.StopWatch() as timer:
                self._run_job()
            self._shutdown.wait(max(0, self.interval_delay - timer.elapsed()))
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

    def _configure(self):
        self.incoming = storage.get_incoming_driver(self.conf.incoming)

    def _run_job(self):
        try:
            report = self.incoming.measures_report(details=False)
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
    GROUP_ID = "gnocchi-processing"

    def __init__(self, worker_id, conf):
        super(MetricProcessor, self).__init__(
            worker_id, conf, conf.metricd.metric_processing_delay)
        self.coord, __ = utils.get_coordinator_and_start(
            conf.storage.coordination_url)
        self._tasks = []
        self.group_state = None

    @utils.retry
    def _configure(self):
        self.store = storage.get_driver(self.conf, self.coord)
        self.index = indexer.get_driver(self.conf)
        self.index.connect()

        # create fallback in case paritioning fails or assigned no tasks
        self.fallback_tasks = list(
            six.moves.range(self.store.incoming.NUM_SACKS))
        try:
            self.partitioner = self.coord.join_partitioned_group(
                self.GROUP_ID, partitions=200)
            LOG.info('Joined coordination group: %s', self.GROUP_ID)

            @periodics.periodic(spacing=self.conf.metricd.worker_sync_rate,
                                run_immediately=True)
            def run_watchers():
                self.coord.run_watchers()

            self.periodic = periodics.PeriodicWorker.create([])
            self.periodic.add(run_watchers)
            t = threading.Thread(target=self.periodic.start)
            t.daemon = True
            t.start()
        except NotImplementedError:
            LOG.warning('Coordinator does not support partitioning. Worker '
                        'will battle against other workers for jobs.')
        except tooz.ToozError as e:
            LOG.error('Unexpected error configuring coordinator for '
                      'partitioning. Retrying: %s', e)
            raise tenacity.TryAgain(e)

    def _get_tasks(self):
        try:
            if (not self._tasks or
                    self.group_state != self.partitioner.ring.nodes):
                self.group_state = self.partitioner.ring.nodes.copy()
                self._tasks = [
                    i for i in six.moves.range(self.store.incoming.NUM_SACKS)
                    if self.partitioner.belongs_to_self(
                        i, replicas=self.conf.metricd.processing_replicas)]
        finally:
            return self._tasks or self.fallback_tasks

    def _run_job(self):
        m_count = 0
        s_count = 0
        in_store = self.store.incoming
        for s in self._get_tasks():
            # TODO(gordc): support delay release lock so we don't
            # process a sack right after another process
            lock = in_store.get_sack_lock(self.coord, s)
            if not lock.acquire(blocking=False):
                continue
            try:
                metrics = in_store.list_metric_with_measures_to_process(s)
                m_count += len(metrics)
                self.store.process_background_tasks(self.index, metrics)
                s_count += 1
            except Exception:
                LOG.error("Unexpected error processing assigned job",
                          exc_info=True)
            finally:
                lock.release()
        LOG.debug("%d metrics processed from %d sacks", m_count, s_count)

    def close_services(self):
        self.coord.stop()


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


def metricd_tester(conf):
    # NOTE(sileht): This method is designed to be profiled, we
    # want to avoid issues with profiler and os.fork(), that
    # why we don't use the MetricdServiceManager.
    index = indexer.get_driver(conf)
    index.connect()
    s = storage.get_driver(conf)
    metrics = set()
    for i in six.moves.range(s.incoming.NUM_SACKS):
        metrics.update(s.incoming.list_metric_with_measures_to_process(i))
        if len(metrics) >= conf.stop_after_processing_metrics:
            break
    s.process_new_measures(
        index, list(metrics)[:conf.stop_after_processing_metrics], True)


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
