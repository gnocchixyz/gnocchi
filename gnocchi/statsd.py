# Copyright (c) 2015 eNovance
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
import itertools
import uuid

try:
    import asyncio
except ImportError:
    import trollius as asyncio
from oslo_config import cfg
from oslo_log import log
import six

from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils


LOG = log.getLogger(__name__)


class Stats(object):
    def __init__(self, conf):
        self.conf = conf
        self.storage = storage.get_driver(self.conf)
        self.indexer = indexer.get_driver(self.conf)
        self.indexer.connect()
        try:
            self.indexer.create_resource('generic',
                                         self.conf.statsd.resource_id,
                                         self.conf.statsd.user_id,
                                         self.conf.statsd.project_id)
        except indexer.ResourceAlreadyExists:
            LOG.debug("Resource %s already exists"
                      % self.conf.statsd.resource_id)
        else:
            LOG.info("Created resource %s" % self.conf.statsd.resource_id)
        self.gauges = {}
        self.counters = {}
        self.times = {}

    def reset(self):
        self.gauges.clear()
        self.counters.clear()
        self.times.clear()

    def treat_metric(self, metric_name, metric_type, value, sampling):
        metric_name += "|" + metric_type
        if metric_type == "ms":
            if sampling is not None:
                raise ValueError(
                    "Invalid sampling for ms: `%d`, should be none"
                    % sampling)
            self.times[metric_name] = storage.Measure(
                utils.utcnow(), value)
        elif metric_type == "g":
            if sampling is not None:
                raise ValueError(
                    "Invalid sampling for g: `%d`, should be none"
                    % sampling)
            self.gauges[metric_name] = storage.Measure(
                utils.utcnow(), value)
        elif metric_type == "c":
            sampling = 1 if sampling is None else sampling
            if metric_name in self.counters:
                current_value = self.counters[metric_name].value
            else:
                current_value = 0
            self.counters[metric_name] = storage.Measure(
                utils.utcnow(),
                current_value + (value * (1 / sampling)))
        # TODO(jd) Support "set" type
        # elif metric_type == "s":
        #     pass
        else:
            raise ValueError("Unknown metric type `%s'" % metric_type)

    def flush(self):
        resource = self.indexer.get_resource('generic',
                                             self.conf.statsd.resource_id,
                                             with_metrics=True)

        for metric_name, measure in itertools.chain(
                six.iteritems(self.gauges),
                six.iteritems(self.counters),
                six.iteritems(self.times)):
            try:
                # NOTE(jd) We avoid considering any concurrency here as statsd
                # is not designed to run in parallel and we do not envision
                # operators manipulating the resource/metrics using the Gnocchi
                # API at the same time.
                metric = resource.get_metric(metric_name)
                if not metric:
                    ap_name = self._get_archive_policy_name(metric_name)
                    metric = self.indexer.create_metric(
                        uuid.uuid4(),
                        self.conf.statsd.user_id,
                        self.conf.statsd.project_id,
                        archive_policy_name=ap_name,
                        name=metric_name,
                        resource_id=self.conf.statsd.resource_id)
                self.storage.add_measures(metric, (measure,))
            except Exception as e:
                LOG.error("Unable to add measure %s: %s"
                          % (metric_name, e))

        self.reset()

    def _get_archive_policy_name(self, metric_name):
        if self.conf.statsd.archive_policy_name:
            return self.conf.statsd.archive_policy_name
        # NOTE(sileht): We didn't catch NoArchivePolicyRuleMatch to log it
        ap = self.indexer.get_archive_policy_for_metric(metric_name)
        return ap.name


class StatsdServer(object):
    def __init__(self, stats):
        self.stats = stats

    @staticmethod
    def connection_made(transport):
        pass

    def datagram_received(self, data, addr):
        LOG.debug("Received data `%r' from %s" % (data, addr))
        try:
            messages = [m for m in data.decode().split("\n") if m]
        except Exception as e:
            LOG.error("Unable to decode datagram: %s" % e)
            return
        for message in messages:
            metric = message.split("|")
            if len(metric) == 2:
                metric_name, metric_type = metric
                sampling = None
            elif len(metric) == 3:
                metric_name, metric_type, sampling = metric
            else:
                LOG.error("Invalid number of | in `%s'" % message)
                continue
            sampling = float(sampling[1:]) if sampling is not None else None
            metric_name, metric_str_val = metric_name.split(':')
            # NOTE(jd): We do not support +/- gauge, and we delete gauge on
            # each flush.
            value = float(metric_str_val)
            try:
                self.stats.treat_metric(metric_name, metric_type,
                                        value, sampling)
            except Exception as e:
                LOG.error("Unable to treat metric %s: %s" % (message, str(e)))


def start():
    conf = service.prepare_service()

    for field in ["resource_id", "user_id", "project_id"]:
        if conf.statsd[field] is None:
            raise cfg.RequiredOptError(field, cfg.OptGroup("statsd"))

    stats = Stats(conf)

    loop = asyncio.get_event_loop()
    # TODO(jd) Add TCP support
    listen = loop.create_datagram_endpoint(
        lambda: StatsdServer(stats),
        local_addr=(conf.statsd.host, conf.statsd.port))

    def _flush():
        loop.call_later(conf.statsd.flush_delay, _flush)
        stats.flush()

    loop.call_later(conf.statsd.flush_delay, _flush)
    transport, protocol = loop.run_until_complete(listen)

    LOG.info("Started on %s:%d" % (conf.statsd.host, conf.statsd.port))
    LOG.info("Flush delay: %d seconds" % conf.statsd.flush_delay)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    transport.close()
    loop.close()
