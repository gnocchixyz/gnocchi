# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import collections
import time

import gnocchi
from gnocchi import incoming
from gnocchi import indexer
from gnocchi.rest import api
from gnocchi import utils

import daiquiri
import numpy
import pecan
from pecan import rest
import pyparsing
import six
import tenacity
try:
    import uwsgi
except ImportError:
    uwsgi = None


LOG = daiquiri.getLogger(__name__)


boolean = "False|True|false|true|FALSE|TRUE|F|T|f|t"
boolean = pyparsing.Regex(boolean).setParseAction(
    lambda t: t[0].lower()[0] == "t")

quoted_string = pyparsing.QuotedString('"', escChar="\\")
unquoted_string = pyparsing.OneOrMore(
    pyparsing.CharsNotIn(" ,=\\") +
    pyparsing.Optional(
        pyparsing.OneOrMore(
            (pyparsing.Literal("\\ ") |
             pyparsing.Literal("\\,") |
             pyparsing.Literal("\\=") |
             pyparsing.Literal("\\")).setParseAction(
                 lambda s, loc, tok: tok[0][-1])))).setParseAction(
                     lambda s, loc, tok: "".join(list(tok)))
measurement = tag_key = tag_value = field_key = quoted_string | unquoted_string
number = r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?"
number = pyparsing.Regex(number).setParseAction(
    lambda s, loc, tok: float(tok[0]))
integer = (
    pyparsing.Word(pyparsing.nums).setParseAction(
        lambda s, loc, tok: int(tok[0])) +
    pyparsing.Suppress("i")
    )
field_value = integer | number | quoted_string
timestamp = pyparsing.Word(pyparsing.nums).setParseAction(
    lambda s, loc, tok: numpy.datetime64(int(tok[0]), 'ns'))

line_protocol = (
    measurement +
    # Tags
    pyparsing.Optional(pyparsing.Suppress(",") +
                       pyparsing.delimitedList(
                           pyparsing.OneOrMore(
                               pyparsing.Group(
                                   tag_key +
                                   pyparsing.Suppress("=") +
                                   tag_value), ",")).setParseAction(
                                       lambda s, loc, tok: dict(list(tok))),
                       default={}) +
    pyparsing.Suppress(" ") +
    # Fields
    pyparsing.delimitedList(
        pyparsing.OneOrMore(
            pyparsing.Group(field_key +
                            pyparsing.Suppress("=") +
                            field_value), ",")).setParseAction(
                                lambda s, loc, tok: dict(list(tok))) +
    # Timestamp
    pyparsing.Optional(pyparsing.Suppress(" ") + timestamp, default=None)
).leaveWhitespace()


query_parser = (
    pyparsing.Suppress(pyparsing.CaselessLiteral("create")) +
    pyparsing.Suppress(pyparsing.CaselessLiteral("database")) +
    pyparsing.Suppress(pyparsing.White()) +
    (pyparsing.QuotedString('"', escChar="\\") |
     pyparsing.Word(pyparsing.alphas + "_",
                    pyparsing.alphanums + "_")) +
    pyparsing.Suppress(
        pyparsing.Optional(pyparsing.Optional(pyparsing.White()) +
                           pyparsing.Optional(pyparsing.Literal(";"))))
)


class InfluxDBController(rest.RestController):
    _custom_actions = {
        'ping': ['HEAD', 'GET'],
        'query': ['POST'],
        'write': ['POST'],
    }

    DEFAULT_TAG_RESOURCE_ID = "host"

    @pecan.expose()
    def ping(self):
        pecan.response.headers['X-Influxdb-Version'] = (
            "Gnocchi " + gnocchi.__version__
        )

    @pecan.expose('json')
    def post_query(self, q=None):
        if q is not None:
            try:
                query = query_parser.parseString(q)
            except pyparsing.ParseException:
                api.abort(501, {"cause": "Not implemented error",
                                "detail": "q",
                                "reason": "Query not implemented"})
            resource_type = query[0]
            api.enforce("create resource type", {"name": resource_type})
            schema = pecan.request.indexer.get_resource_type_schema()
            rt = schema.resource_type_from_dict(resource_type, {}, 'creating')
            try:
                pecan.request.indexer.create_resource_type(rt)
            except indexer.ResourceTypeAlreadyExists:
                pass
            pecan.response.status = 204

    @staticmethod
    def _write_get_lines():
        encoding = pecan.request.headers.get('Transfer-Encoding', "").lower()
        if encoding == "chunked":
            # TODO(sileht): Support reading chunk without uwsgi when
            # pecan.request.environ['wsgi.input_terminated'] is set.
            # https://github.com/unbit/uwsgi/issues/1428
            if uwsgi is None:
                api.abort(
                    501, {"cause": "Not implemented error",
                          "reason": "This server is not running with uwsgi"})
            return encoding, uwsgi.chunked_read()
        return None, pecan.request.body

    @pecan.expose('json')
    def post_write(self, db="influxdb"):

        creator = pecan.request.auth_helper.get_current_user(pecan.request)
        tag_to_rid = pecan.request.headers.get(
            "X-Gnocchi-InfluxDB-Tag-Resource-ID",
            self.DEFAULT_TAG_RESOURCE_ID)

        while True:
            encoding, chunk = self._write_get_lines()

            # If chunk is empty then this is over.
            if not chunk:
                break

            # Compute now on a per-chunk basis
            now = numpy.datetime64(int(time.time() * 10e8), 'ns')

            # resources = { resource_id: {
            #     metric_name: [ incoming.Measure(t, v), …], …
            #   }, …
            # }
            resources = collections.defaultdict(
                lambda: collections.defaultdict(list))
            for line_number, line in enumerate(chunk.split(b"\n")):
                # Ignore empty lines
                if not line:
                    continue

                try:
                    measurement, tags, fields, timestamp = (
                        line_protocol.parseString(line.decode())
                    )
                except (UnicodeDecodeError, SyntaxError,
                        pyparsing.ParseException):
                    api.abort(400, {
                        "cause": "Value error",
                        "detail": "line",
                        "reason": "Unable to parse line %d" % (
                            line_number + 1),
                    })

                if timestamp is None:
                    timestamp = now

                try:
                    resource_id = tags.pop(tag_to_rid)
                except KeyError:
                    api.abort(400, {
                        "cause": "Value error",
                        "detail": "key",
                        "reason": "Unable to find key `%s' in tags" % (
                            tag_to_rid),
                    })

                tags_str = (("@" if tags else "") +
                            ",".join(("%s=%s" % (k, tags[k]))
                                     for k in sorted(tags)))

                for field_name, field_value in six.iteritems(fields):
                    if isinstance(field_value, str):
                        # We do not support field value that are not numerical
                        continue

                    # Metric name is the:
                    # <measurement>.<field_key>@<tag_key>=<tag_value>,…
                    # with tag ordered
                    # Replace "/" with "_" because Gnocchi does not support /
                    # in metric names
                    metric_name = (
                        measurement + "." + field_name + tags_str
                    ).replace("/", "_")

                    resources[resource_id][metric_name].append(
                        incoming.Measure(timestamp, field_value))

            measures_to_batch = {}
            for resource_name, metrics_and_measures in six.iteritems(
                    resources):
                resource_name = resource_name
                resource_id = utils.ResourceUUID(
                    resource_name, creator=creator)
                LOG.debug("Getting metrics from resource `%s'", resource_name)
                timeout = pecan.request.conf.api.operation_timeout
                metrics = (
                    api.get_or_create_resource_and_metrics.retry_with(
                        stop=tenacity.stop_after_delay(timeout))(
                            creator, resource_id, resource_name,
                            metrics_and_measures.keys(),
                            {}, db)
                )

                for metric in metrics:
                    api.enforce("post measures", metric)

                measures_to_batch.update(
                    dict((metric.id, metrics_and_measures[metric.name])
                         for metric in metrics
                         if metric.name in metrics_and_measures))

            LOG.debug("Add measures batch for %d metrics",
                      len(measures_to_batch))
            pecan.request.incoming.add_measures_batch(measures_to_batch)
            pecan.response.status = 204

            if encoding != "chunked":
                return
