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
from __future__ import print_function
import itertools
import ujson
import uuid

import daiquiri
from oslo_config import cfg

import six

from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service

from gnocchi import utils

from proton.handlers import MessagingHandler
from proton.reactor import Container

LOG = daiquiri.getLogger(__name__)


class CollectdStats(object):
    def __init__(self, conf):
        self.conf = conf
        self.incoming = incoming.get_driver(self.conf)
        self.indexer = indexer.get_driver(self.conf)
        self.resource_type = None

        self._ensure_resource_type_exists()
        self.gauges = {}
        self.counters = {}
        self.absolute = {}

    def reset(self):
        self.gauges.clear()
        self.counters.clear()
        self.absolute.clear()

    def on_timer_task(self, event):
        event.container.schedule(self.conf.amqp1d.flush_delay, self)
        self.flush()

    @staticmethod
    def serialize_identifier(index, value):
        """Based of FORMAT_VL from collectd/src/daemon/common.h.

        The biggest difference is that we don't prepend the host and append the
        index of the value, and don't use slash.

        """
        return (value["plugin"] + ("-" + value["plugin_instance"]
                                   if value["plugin_instance"] else "")
                + "@"
                + value["type"] + ("-" + value["type_instance"]
                                   if value["type_instance"] else "")
                + "-" + str(index))

    def treat_metric(self, host, metric_name, metric_type,
                     value):
        """Collectd.

        Statistics in collectd consist of a value list. A value list includes:
        Values, can be one of:
        Derive: used for values where a change in the value since it's last
        been read is of interest. Can be used to calculate and store a rate.
        Counter: similar to derive values, but take the possibility of a
        counter wrap around into consideration.
        Gauge: used for values that are stored as is.
        Absolute: used for counters that are reset after reading.

        """

        if metric_type == "absolute":
            if host not in self.absolute:
                self.absolute[host] = {}
            self.absolute[host][metric_name] = incoming.Measure(
                utils.dt_in_unix_ns(utils.utcnow()), value)
        elif metric_type == "gauge":
            if host not in self.gauges:
                self.gauges[host] = {}
            self.gauges[host][metric_name] = incoming.Measure(
                utils.dt_in_unix_ns(utils.utcnow()), value)
        elif metric_type == "counter" or metric_type == "derive":
            if host not in self.counters:
                self.counters[host] = {}
            self.counters[host][metric_name] = incoming.Measure(
                utils.dt_in_unix_ns(utils.utcnow()), value)
        else:
            raise ValueError("Unknown metric type '%s'" % metric_type)

    def flush(self):
        for host, measures in itertools.chain(
            six.iteritems(
                self.counters), six.iteritems(
                self.gauges), six.iteritems(
                self.absolute)):

            host_id = self.conf.amqp1d.resource_name + \
                ":" + host.replace("/", "_")
            resources = self.indexer.list_resources(
                self.conf.amqp1d.resource_name,
                attribute_filter={"=": {"host": host}})
            if not resources:
                self._ensure_resource_exists(host_id, host)
                resources = self.indexer.list_resources(
                    self.conf.amqp1d.resource_name,
                    attribute_filter={"=": {"host": host}})

            resource = self.indexer.get_resource(
                self.conf.amqp1d.resource_name,
                resources[0].id, with_metrics=True)
            for name in measures:
                metric_name = name.split(" ")
                if len(metric_name) == 2:
                    metric_name = metric_name[1]
                else:
                    metric_name = name
                metric = resource.get_metric(metric_name)
                try:
                    if not metric:
                        ap_name = self._get_archive_policy_name(
                            metric_name)
                        metric = self.indexer.create_metric(
                            uuid.uuid4(),
                            self.conf.amqp1d.creator,
                            archive_policy_name=ap_name,
                            name=metric_name,
                            resource_id=resource.id)
                    self.incoming.add_measures(metric, (measures[name],))
                except Exception as exception:
                    LOG.error("Unable to add measure %s: %s",
                              metric_name, exception)

        self.reset()

    def _ensure_resource_exists(self, host_id, host):
        try:
            self.indexer.create_resource(
                self.conf.amqp1d.resource_name,
                utils.ResourceUUID(
                    host_id,
                    self.conf.amqp1d.creator),
                self.conf.amqp1d.creator,
                original_resource_id=host_id,
                host=host)
        except indexer.ResourceAlreadyExists:
            pass
            # LOG.debug("Resource %s already exists", host_id)
        else:
            LOG.info("Created resource for %s", host)

    def _ensure_resource_type_exists(self):
        try:
            self.resource_type = self.indexer.get_resource_type(
                self.conf.amqp1d.resource_name)
        except indexer.NoSuchResourceType:
            pass
        if self.resource_type is None:
            try:
                mgr = self.indexer.get_resource_type_schema()
                rtype = mgr.resource_type_from_dict(
                    self.conf.amqp1d.resource_name, {
                        "host": {"type": "string", "required": True,
                                 "min_length": 0, "max_length": 255},
                    }, "creating")
                self.indexer.create_resource_type(rtype)
            except indexer.ResourceTypeAlreadyExists:
                LOG.debug("Resource type %s already exists",
                          self.conf.amqp1d.resource_name)
            else:
                LOG.info(
                    "Created resource type %s",
                    self.conf.amqp1d.resource_name)
                self.resource_type = self.indexer.get_resource_type(
                    self.conf.amqp1d.resource_name)
        else:
            LOG.info(
                "Found resource type %s",
                self.conf.amqp1d.resource_name)

    def _get_archive_policy_name(self, metric_name):
        if self.conf.amqp1d.archive_policy_name:
            return self.conf.amqp1d.archive_policy_name
        # NOTE(sileht): We didn't catch NoArchivePolicyRuleMatch to log it
        archive_policy = self.indexer.get_archive_policy_for_metric(
            metric_name)
        return archive_policy.name


class AMQP1Server(MessagingHandler):
    def __init__(self, conf, stats):
        super(AMQP1Server, self).__init__()
        self.stats = stats
        self.url = conf.amqp1d.host + ":" + \
            str(conf.amqp1d.port) + "/" + conf.amqp1d.topic
        self.conf = conf
        self.peer_close_is_error = True

    def on_start(self, event):
        event.container.create_receiver(self.url)
        event.container.schedule(self.conf.amqp1d.flush_delay, self.stats)

    def on_message(self, event):
        # not handling duplicate
        # if self.amqp1d.data_source == "collectd":
        self.process_collectd_message(event.message.body)

    def process_collectd_message(self, messagebody):
        json_message = ujson.loads(messagebody)
        for message in json_message:
            for index, value in enumerate(message["values"]):
                try:
                    self.stats.treat_metric(
                        message["host"], self.stats.serialize_identifier(
                            index, message), message["dstypes"][index], value)
                except Exception as exception:
                    LOG.error(
                        "Unable to treat metric %s: %s",
                        message,
                        str(exception))


def start():
    conf = service.prepare_service()
    if conf.amqp1d.resource_name is None:
        raise cfg.RequiredOptError("resource_name", cfg.OptGroup("amqp1d"))
    try:
        if conf.amqp1d.data_source == "collectd":
            stats = CollectdStats(conf)
            Container(AMQP1Server(conf, stats)).run()
        else:
            raise ValueError(
                "Unknown data source type '%s'" %
                conf.amqp1d.data_source)
    except KeyboardInterrupt:
        pass
