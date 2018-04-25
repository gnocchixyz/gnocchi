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
import socket
import threading
import time
import uuid

import cachetools.func
import cotyledon
from cotyledon import oslo_config_glue
import daiquiri
from oslo_config import cfg
import tenacity
import tooz
from tooz import coordination

from gnocchi import chef
from gnocchi import exceptions
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils


LOG = daiquiri.getLogger(__name__)


@utils.retry_on_exception_and_log("Unable to initialize coordination driver")
def get_coordinator_and_start(member_id, url):
    coord = coordination.get_coordinator(url, member_id)
    coord.start(start_heart=True)
    return coord


class MetricProcessBase(cotyledon.Service):
    def __init__(self, worker_id, conf, interval_delay=0):
        super(MetricProcessBase, self).__init__(worker_id)
        self.conf = conf
        self.startup_delay = self.worker_id = worker_id
        self.interval_delay = interval_delay
        self._wake_up = threading.Event()
        self._shutdown = threading.Event()
        self._shutdown_done = threading.Event()

    def wakeup(self):
        self._wake_up.set()

    def _configure(self):
        member_id = "%s.%s.%s" % (socket.gethostname(),
                                  self.worker_id,
                                  # NOTE(jd) Still use a uuid here so we're
                                  # sure there's no conflict in case of
                                  # crash/restart
                                  str(uuid.uuid4()))
        self.coord = get_coordinator_and_start(member_id,
                                               self.conf.coordination_url)
        self.store = storage.get_driver(self.conf)
        self.incoming = incoming.get_driver(self.conf)
        self.index = indexer.get_driver(self.conf)
        self.chef = chef.Chef(self.coord, self.incoming,
                              self.index, self.store)

    def run(self):
        self._configure()
        # Delay startup so workers are jittered.
        time.sleep(self.startup_delay)

        while not self._shutdown.is_set():
            with utils.StopWatch() as timer:
                try:
                    self._run_job()
                except Exception:
                    LOG.error("Unexpected error during %s job",
                              self.name,
                              exc_info=True)
            self._wake_up.wait(max(0, self.interval_delay - timer.elapsed()))
            self._wake_up.clear()
        self._shutdown_done.set()

    def terminate(self):
        self._shutdown.set()
        self.wakeup()
        LOG.info("Waiting ongoing metric processing to finish")
        self._shutdown_done.wait()
        self.close_services()

    def close_services(self):
        self.coord.stop()

    @staticmethod
    def _run_job():
        raise NotImplementedError


class MetricReporting(MetricProcessBase):
    name = "reporting"

    def __init__(self, worker_id, conf):
        super(MetricReporting, self).__init__(
            worker_id, conf, conf.metricd.metric_reporting_delay)

    def _configure(self):
        self.incoming = incoming.get_driver(self.conf)

    @staticmethod
    def close_services():
        pass

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


class MetricProcessor(MetricProcessBase):
    name = "processing"
    GROUP_ID = b"gnocchi-processing"

    def __init__(self, worker_id, conf):
        super(MetricProcessor, self).__init__(
            worker_id, conf, conf.metricd.metric_processing_delay)
        self._tasks = []
        self.group_state = None
        self.sacks_with_measures_to_process = set()
        # This stores the last time the processor did a scan on all the sack it
        # is responsible for
        self._last_full_sack_scan = utils.StopWatch().start()
        # Only update the list of sacks to process every
        # metric_processing_delay
        self._get_sacks_to_process = cachetools.func.ttl_cache(
            ttl=conf.metricd.metric_processing_delay
        )(self._get_sacks_to_process)

    @tenacity.retry(
        wait=utils.wait_exponential,
        # Never retry except when explicitly asked by raising TryAgain
        retry=tenacity.retry_never)
    def _configure(self):
        super(MetricProcessor, self)._configure()

        # create fallback in case paritioning fails or assigned no tasks
        self.fallback_tasks = list(self.incoming.iter_sacks())
        try:
            self.partitioner = self.coord.join_partitioned_group(
                self.GROUP_ID, partitions=200)
            LOG.info('Joined coordination group: %s',
                     self.GROUP_ID.decode())
        except tooz.NotImplemented:
            LOG.warning('Coordinator does not support partitioning. Worker '
                        'will battle against other workers for jobs.')
        except tooz.ToozError as e:
            LOG.error('Unexpected error configuring coordinator for '
                      'partitioning. Retrying: %s', e)
            raise tenacity.TryAgain(e)

        if self.conf.metricd.greedy:
            filler = threading.Thread(target=self._fill_sacks_to_process)
            filler.daemon = True
            filler.start()

    @utils.retry_on_exception.wraps
    def _fill_sacks_to_process(self):
        try:
            for sack in self.incoming.iter_on_sacks_to_process():
                if sack in self._get_sacks_to_process():
                    LOG.debug(
                        "Got notification for sack %s, waking up processing",
                        sack)
                    self.sacks_with_measures_to_process.add(sack)
                    self.wakeup()
        except exceptions.NotImplementedError:
            LOG.info("Incoming driver does not support notification")
        except Exception as e:
            LOG.error(
                "Error while listening for new measures notification, "
                "retrying",
                exc_info=True)
            raise tenacity.TryAgain(e)

    def _get_sacks_to_process(self):
        try:
            self.coord.run_watchers()
            if (not self._tasks or
                    self.group_state != self.partitioner.ring.nodes):
                self.group_state = self.partitioner.ring.nodes.copy()
                self._tasks = [
                    sack for sack in self.incoming.iter_sacks()
                    if self.partitioner.belongs_to_self(
                        sack, replicas=self.conf.metricd.processing_replicas)]
        except tooz.NotImplemented:
            # Do not log anything. If `run_watchers` is not implemented, it's
            # likely that partitioning is not implemented either, so it already
            # has been logged at startup with a warning.
            pass
        except Exception as e:
            LOG.error('Unexpected error updating the task partitioner: %s', e)
        finally:
            return self._tasks or self.fallback_tasks

    def _run_job(self):
        m_count = 0
        s_count = 0
        # We are going to process the sacks we got notified for, and if we got
        # no notification, then we'll just try to process them all, just to be
        # sure we don't miss anything. In case we did not do a full scan for
        # more than `metric_processing_delay`, we do that instead.
        if self._last_full_sack_scan.elapsed() >= self.interval_delay:
            sacks = self._get_sacks_to_process()
        else:
            sacks = (self.sacks_with_measures_to_process.copy()
                     or self._get_sacks_to_process())
        for s in sacks:
            try:
                try:
                    m_count += self.chef.process_new_measures_for_sack(s)
                except chef.SackAlreadyLocked:
                    continue
                s_count += 1
                self.incoming.finish_sack_processing(s)
                self.sacks_with_measures_to_process.discard(s)
            except Exception:
                LOG.error("Unexpected error processing assigned job",
                          exc_info=True)
        LOG.debug("%d metrics processed from %d sacks", m_count, s_count)
        # Update statistics
        self.coord.update_capabilities(self.GROUP_ID, self.store.statistics)
        if sacks == self._get_sacks_to_process():
            # We just did a full scan of all sacks, reset the timer
            self._last_full_sack_scan.reset()
            LOG.debug("Full scan of sacks has been done")

    def close_services(self):
        self.coord.stop()


class MetricJanitor(MetricProcessBase):
    name = "janitor"

    def __init__(self,  worker_id, conf):
        super(MetricJanitor, self).__init__(
            worker_id, conf, conf.metricd.metric_cleanup_delay)

    def _run_job(self):
        self.chef.expunge_metrics()
        LOG.debug("Metrics marked for deletion removed from backend")


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
    s = storage.get_driver(conf)
    inc = incoming.get_driver(conf)
    c = chef.Chef(None, inc, index, s)
    metrics_count = 0
    for sack in inc.iter_sacks():
        try:
            metrics_count += c.process_new_measures_for_sack(s, True)
        except chef.SackAlreadyLocked:
            continue
        if metrics_count >= conf.stop_after_processing_metrics:
            break


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
