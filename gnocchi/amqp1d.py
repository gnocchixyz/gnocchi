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

import collections
import itertools
import uuid

import daiquiri
import proton.handlers
import proton.reactor
import six
import ujson

from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


class BatchProcessor(object):
    def __init__(self, conf):
        self.conf = conf
        self.incoming = incoming.get_driver(self.conf)
        self.indexer = indexer.get_driver(self.conf)
        self._ensure_resource_type_exists()

        self._hosts = {}
        self._measures = collections.defaultdict(
            lambda: collections.defaultdict(list))

    def reset(self):
        self._hosts.clear()
        self._measures.clear()

    def add_measures(self, host, name, measures):
        host_id = "%s:%s" % (self.conf.amqp1d.resource_type,
                             host.replace("/", "_"))
        self._hosts[host_id] = host
        self._measures[host_id][name].extend(measures)

    def flush(self):
        try:
            self._flush()
        except Exception:
            LOG.error("Unepected error during flush()", exc_info=True)
        self.reset()

    def _flush(self):
        archive_policies = {}
        resources = self._get_resources(self._measures.keys())
        for host_id, measures_by_names in six.iteritems(self._measures):
            resource = resources[host_id]

            names = set(measures_by_names.keys())
            for name in names:
                if name not in archive_policies:
                    archive_policies[name] = (
                        self.indexer.get_archive_policy_for_metric(name))
            known_metrics = self.indexer.list_metrics(attribute_filter={
                "and": [{"=": {"resource_id": resource.id}},
                        {"in": {"name": list(names)}}]
            })
            known_names = set((m.name for m in known_metrics))
            already_exists_names = []
            for name in (names - known_names):
                try:
                    m = self.indexer.create_metric(
                        uuid.uuid4(),
                        creator=self.conf.amqp1d.creator,
                        resource_id=resource.id,
                        name=name,
                        archive_policy_name=archive_policies[name].name)
                except indexer.NamedMetricAlreadyExists as e:
                    already_exists_names.append(e.metric)
                except indexer.IndexerException as e:
                    LOG.error("Unexpected error, dropping metric %s",
                              name, exc_info=True)
                else:
                    known_metrics.append(m)

            if already_exists_names:
                # Add metrics created in the meantime
                known_names.extend(already_exists_names)
                known_metrics.extend(
                    self.indexer.list_metrics(attribute_filter={
                        "and": [{"=": {"resource_id": resource.id}},
                                {"in": {"name": already_exists_names}}]
                    }))

            self.incoming.add_measures_batch(
                dict((metric.id,
                     measures_by_names[metric.name])
                     for metric in known_metrics))

    def _get_resources(self, host_ids):

        resource_ids = set((utils.ResourceUUID(host_id,
                                               self.conf.amqp1d.creator)
                            for host_id in host_ids))

        resources = self.indexer.list_resources(
            resource_type=self.conf.amqp1d.resource_type,
            attribute_filter={"in": {"id": resource_ids}})

        resources_by_host_id = {r.original_resource_id: r for r in resources}

        missing_host_ids = set(host_ids) - set(resources_by_host_id.keys())

        for host_id in missing_host_ids:
            resource_id = utils.ResourceUUID(host_id,
                                             self.conf.amqp1d.creator)
            try:
                r = self.indexer.create_resource(
                    self.conf.amqp1d.resource_type,
                    resource_id,
                    self.conf.amqp1d.creator,
                    original_resource_id=host_id,
                    host=self._hosts[host_id])
            except indexer.ResourceAlreadyExists:
                r = self.indexer.get_resource(
                    self.conf.amqp1d.resource_type,
                    resource_id)
            resources_by_host_id[host_id] = r

        return resources_by_host_id

    def _ensure_resource_type_exists(self):
        try:
            self.resource_type = self.indexer.get_resource_type(
                self.conf.amqp1d.resource_type)
        except indexer.NoSuchResourceType:
            try:
                mgr = self.indexer.get_resource_type_schema()
                rtype = mgr.resource_type_from_dict(
                    self.conf.amqp1d.resource_type, {
                        "host": {"type": "string", "required": True,
                                 "min_length": 0, "max_length": 255},
                    }, "creating")
                self.indexer.create_resource_type(rtype)
            except indexer.ResourceTypeAlreadyExists:
                LOG.debug("Resource type %s already exists",
                          self.conf.amqp1d.resource_type)
            else:
                LOG.info("Created resource type %s",
                         self.conf.amqp1d.resource_type)
                self.resource_type = self.indexer.get_resource_type(
                    self.conf.amqp1d.resource_type)
        else:
            LOG.info("Found resource type %s",
                     self.conf.amqp1d.resource_type)


class CollectdFormatHandler(object):
    def __init__(self, processor):
        self.processor = processor

    @staticmethod
    def _serialize_identifier(index, message):
        """Based of FORMAT_VL from collectd/src/daemon/common.h.

        The biggest difference is that we don't prepend the host and append the
        index of the value, and don't use slash.

        """
        suffix = ("-%s" % message["dsnames"][index]
                  if len(message["dsnames"]) > 1 else "")
        return (message["plugin"] + ("-" + message["plugin_instance"]
                                     if message["plugin_instance"] else "")
                + "@"
                + message["type"] + ("-" + message["type_instance"]
                                     if message["type_instance"] else "")
                + suffix)

    def on_message(self, event):
        json_message = ujson.loads(event.message.body)
        timestamp = utils.dt_in_unix_ns(utils.utcnow())
        measures_by_host_and_name = sorted((
            (message["host"],
             self._serialize_identifier(index, message),
             value)
            for message in json_message
            for index, value in enumerate(message["values"])
        ))
        for (host, name), values in itertools.groupby(
                measures_by_host_and_name, key=lambda x: x[0:2]):
            measures = (incoming.Measure(timestamp, v[2]) for v in values)
            self.processor.add_measures(host, name, measures)


class AMQP1Server(proton.handlers.MessagingHandler):

    def __init__(self, conf):
        super(AMQP1Server, self).__init__()
        self.peer_close_is_error = True
        self.conf = conf

        self.processor = BatchProcessor(conf)

        # Only collectd format is supported for now
        self.data_source_handler = {
            "collectd": CollectdFormatHandler
        }[self.conf.amqp1d.data_source](self.processor)

    def on_start(self, event):
        event.container.schedule(self.conf.amqp1d.flush_delay, self)

    def on_message(self, event):
        self.data_source_handler.on_message(event)

    def on_timer_task(self, event):
        event.container.schedule(self.conf.amqp1d.flush_delay, self)
        self.processor.flush()


def start():
    conf = service.prepare_service()
    server = proton.reactor.Container(AMQP1Server(conf))
    try:
        server.run()
    except KeyboardInterrupt:
        pass
