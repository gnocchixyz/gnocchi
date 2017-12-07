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
import uuid

import falcon
from falcon import status_codes
import marshmallow
import numpy
from oslo_policy import policy
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import driver
import voluptuous

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import json
from gnocchi.rest import api
from gnocchi import storage
from gnocchi import utils


def enforce(policy_enforcer, auth_info, rule, target):
    """Return the user and project the request should be limited to.

    :param policy_enforcer: The policy enforcer.
    :param auth_info: The user credentials.
    :param rule: The rule name.
    :param target: The target to enforce on.

    """
    if not isinstance(target, dict):
        if hasattr(target, "jsonify"):
            target = target.jsonify()
        else:
            target = target.__dict__

    # Flatten dict
    target = dict(api.flatten_dict_to_keypairs(d=target, separator='.'))

    if not policy_enforcer.enforce(rule, target, auth_info):
        abort(403)


def deserialize(req, expected_content_types=("application/json",)):
    if req.content_type not in expected_content_types:
        abort(415)
    try:
        return json.load(req.stream)
    except Exception as e:
        abort(400, "Unable to decode body: " + six.text_type(e))


def get_pagination_options(params, max_limit, default):
    try:
        opts = voluptuous.Schema({
            voluptuous.Required(
                "limit", default=max_limit):
            voluptuous.All(voluptuous.Coerce(int),
                           voluptuous.Range(min=1),
                           voluptuous.Clamp(
                               min=1, max=max_limit)),
            "marker": six.text_type,
            voluptuous.Required("sort", default=default):
            voluptuous.All(
                voluptuous.Coerce(api.arg_to_list),
                [six.text_type]),
        }, extra=voluptuous.REMOVE_EXTRA)(params)
    except voluptuous.Invalid as e:
        abort(400, {"cause": "Argument value error",
                    "reason": str(e)})
    opts['sorts'] = opts['sort']
    del opts['sort']
    return opts


def abort(status_code, detail=''):
    if status_code == 404 and not detail:
        raise RuntimeError("http code 404 must have 'detail' set")
    if isinstance(detail, Exception):
        detail = detail.jsonify()
    raise falcon.HTTPError(getattr(status_codes, "HTTP_" + str(status_code)),
                           description=detail)


def set_resp_location_hdr(req, resp, location):
    location = '%s%s' % (req.app, location)
    # NOTE(sileht): according the pep-3333 the headers must be
    # str in py2 and py3 even this is not the same thing in both
    # version
    # see: http://legacy.python.org/dev/peps/pep-3333/#unicode-issues
    if six.PY2 and isinstance(location, six.text_type):
        location = location.encode('utf-8')
    location = urllib_parse.quote(location)
    resp.set_header('Location', location)


def set_resp_link_hdr(req, resp, marker, *args):
    # NOTE(sileht): This comes from rfc5988.
    # Setting prev, last is too costly/complicated, so just set next for now.
    options = {}
    for arg in args:
        options.update(arg)
    if "sorts" in options:
        options["sort"] = options["sorts"]
        del options["sorts"]
    options["marker"] = marker
    # NOTE(sileht): To always have the same orders
    options = sorted(options.items())
    params = urllib_parse.urlencode(options, doseq=True)
    resp.add_link('%s://%s%s?%s' % (
        req.scheme, req.netloc, req.app, params
    ), "next")


class ResourceIDField(marshmallow.fields.Field):
    def _deserialize(self, value, attr, data):
        return (six.text_type(value),
                utils.ResourceUUID(value, self.context['creator']))


class TimespanField(marshmallow.fields.Field):
    def _deserialize(self, value, attr, data):
        return utils.to_timespan(value)

    def _serialize(self, value, attr, data):
        # FIXME(jd) provide that in gnocchi.utils
        if value is not None:
            return value / numpy.timedelta64(1, 's')


class ArchivePolicyDefinitionSchema(marshmallow.Schema):
    granularity = TimespanField()
    timespan = TimespanField()
    points = marshmallow.fields.Integer(
        validate=marshmallow.validate.Range(min=1))


class ArchivePolicySchema(marshmallow.Schema):
    name = marshmallow.fields.String(required=True)
    back_window = marshmallow.fields.Integer(
        validate=marshmallow.validate.Range(min=0),
        missing=0)
    aggregation_methods = marshmallow.fields.List(
        marshmallow.fields.String(
            validate=marshmallow.validate.OneOf(
                archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS_VALUES
            )))
    definition = marshmallow.fields.Nested(
        ArchivePolicyDefinitionSchema, many=True)


class MetricSchema(marshmallow.Schema):
    id = marshmallow.fields.UUID(dump_only=True)
    creator = marshmallow.fields.String(dump_only=True)
    archive_policy_name = marshmallow.fields.String()
    archive_policy = marshmallow.fields.Nested(
        ArchivePolicySchema(), dump_only=True)
    resource_id = ResourceIDField()
    name = marshmallow.fields.String()
    unit = marshmallow.fields.String(
        validate=marshmallow.validate.Length(max=31))


MetricSchemaI = MetricSchema()


def dump_with_schema(data, schema, many=False):
    marshal = schema.dump(data, many=many)
    if marshal.errors:
        raise RuntimeError(
            "Error while serializing data with schema %r" % schema)
    return json.dumps(marshal.data)


class MetricsResource(object):

    MetricListSchema = voluptuous.Schema({
        "user_id": six.text_type,
        "project_id": six.text_type,
        "creator": six.text_type,
        "name": six.text_type,
        "id": six.text_type,
        "unit": six.text_type,
        "archive_policy_name": six.text_type,
        "status": voluptuous.Any("active", "delete"),
    }, extra=voluptuous.REMOVE_EXTRA)

    def __init__(self, policy_enforcer, auth_helper, indexer, max_limit):
        self.policy_enforcer = policy_enforcer
        self.auth_helper = auth_helper
        self.indexer = indexer
        self.max_limit = max_limit

    def on_get(self, req, resp):
        filtering = self.MetricListSchema(req.params)

        # Compat with old user/project API
        provided_user_id = filtering.pop('user_id', None)
        provided_project_id = filtering.pop('project_id', None)
        if provided_user_id is None and provided_project_id is None:
            provided_creator = filtering.pop('creator', None)
        else:
            provided_creator = (
                (provided_user_id or "")
                + ":"
                + (provided_project_id or "")
            )

        pagination_opts = get_pagination_options(
            req.params, self.max_limit, api.METRIC_DEFAULT_PAGINATION)
        attr_filters = []
        if provided_creator is not None:
            attr_filters.append({"=": {"creator": provided_creator}})

        for k, v in six.iteritems(filtering):
            attr_filters.append({"=": {k: v}})

        policy_filter = self.auth_helper.get_metric_policy_filter(
            req, "list metric")
        resource_policy_filter = (
            self.auth_helper.get_resource_policy_filter(
                req, "list metric", resource_type=None,
                prefix="resource")
        )

        try:
            metrics = self.indexer.list_metrics(
                attribute_filter={"and": attr_filters},
                policy_filter=policy_filter,
                resource_policy_filter=resource_policy_filter,
                **pagination_opts)
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))

        if metrics and len(metrics) >= pagination_opts['limit']:
            # HACK(jd) replace by req.path when the root is the API and not
            # metric
            set_resp_link_hdr(req, resp,
                              str(metrics[-1].id), req.params, pagination_opts)

        resp.body = dump_with_schema(metrics, MetricSchemaI, many=True)

    # NOTE(jd) Define this method as it was a voluptuous schema – it's just a
    # smarter version of a voluptuous schema, no?
    def MetricSchema(self, definition, auth_info, creator):
        # First basic validation
        unmarshal = MetricSchema(context={"creator": creator}).load(definition)
        if unmarshal.errors:
            abort(400,
                  [{"cause": "Attribute value error",
                    "detail": field_name,
                    "reason": reasons}
                   for field_name, reasons
                   in six.iteritems(unmarshal.errors)])

        definition = unmarshal.data
        archive_policy_name = definition.get('archive_policy_name')

        name = definition.get('name')
        if name and '/' in name:
            abort(400, "'/' is not supported in metric name")
        if archive_policy_name is None:
            try:
                ap = self.indexer.get_archive_policy_for_metric(name)
            except indexer.NoArchivePolicyRuleMatch:
                # NOTE(jd) Since this is a schema-like function, we
                # should/could raise ValueError, but if we do so, voluptuous
                # just returns a "invalid value" with no useful message – so we
                # prefer to use abort() to make sure the user has the right
                # error message
                abort(400, "No archive policy name specified "
                      "and no archive policy rule found matching "
                      "the metric name %s" % name)
            else:
                definition['archive_policy_name'] = ap.name

        resource_id = definition.get('resource_id')
        if resource_id is None:
            original_resource_id = None
        else:
            if name is None:
                abort(400,
                      {"cause": "Attribute value error",
                       "detail": "name",
                       "reason": "Name cannot be null "
                       "if resource_id is not null"})
            original_resource_id, resource_id = resource_id

        enforce(self.policy_enforcer, auth_info, "create metric", {
            "creator": creator,
            "archive_policy_name": archive_policy_name,
            "resource_id": resource_id,
            "original_resource_id": original_resource_id,
            "name": name,
            "unit": definition.get('unit'),
        })

        return definition

    def on_post(self, req, resp):
        creator = self.auth_helper.get_current_user(req)
        auth_info = self.auth_helper.get_auth_info(req)

        body = self.MetricSchema(
            deserialize(req),
            auth_info=auth_info,
            creator=creator)

        resource_id = body.get('resource_id')
        if resource_id is not None:
            resource_id = resource_id[1]

        try:
            m = self.indexer.create_metric(
                uuid.uuid4(),
                creator,
                resource_id=resource_id,
                name=body.get('name'),
                unit=body.get('unit'),
                archive_policy_name=body['archive_policy_name'])
        except indexer.NoSuchArchivePolicy as e:
            abort(400, six.text_type(e))
        except indexer.NamedMetricAlreadyExists as e:
            abort(400, e)
        set_resp_location_hdr(req, resp, "/" + str(m.id))
        resp.status = 201
        # FIXME(jd) Need to call jsonify because of archive_policy not loaded
        # here.
        resp.body = dump_with_schema(m.jsonify(), MetricSchemaI)


class MetricResourceBase(object):
    def __init__(self, policy_enforcer, auth_helper, indexer):
        self.policy_enforcer = policy_enforcer
        self.auth_helper = auth_helper
        self.indexer = indexer

    def _get_metric(self, enforce_rule, metric_id, req):
        metrics = self.indexer.list_metrics(
            attribute_filter={"=": {"id": metric_id}},
            details=True)
        if not metrics:
            abort(404, six.text_type(indexer.NoSuchMetric(metric_id)))
        metric = metrics[0]
        auth_info = self.auth_helper.get_auth_info(req)
        enforce(self.policy_enforcer, auth_info,
                enforce_rule, json.to_primitive(metric))
        return metric


class MetricResource(MetricResourceBase):
    def on_get(self, req, resp, metric_id):
        metric = self._get_metric("get metric", metric_id, req)
        resp.body = dump_with_schema(metric, MetricSchemaI)

    def on_delete(self, req, resp, metric_id):
        metric = self._get_metric("get metric", metric_id, req)
        try:
            self.indexer.delete_metric(metric.id)
        except indexer.NoSuchMetric as e:
            abort(404, six.text_type(e))
        resp.status = 204


class MeasuresResource(MetricResourceBase):
    def __init__(self, policy_enforcer, auth_helper, indexer,
                 storage, incoming, operation_timeout):
        super(MeasuresResource, self).__init__(
            policy_enforcer, auth_helper, indexer)
        self.storage = storage
        self.incoming = incoming
        self.operation_timeout = operation_timeout

    def on_post(self, req, resp, metric_id):
        metric = self._get_metric("post measures", metric_id, req)
        params = deserialize(req)
        if not isinstance(params, list):
            abort(400, "Invalid input for measures")
        if params:
            self.incoming.add_measures(
                metric, api.MeasuresListSchema(params))
        resp.status = 202

    def on_get(self, req, resp, metric_id):
        metric = self._get_metric("get measures", metric_id, req)
        aggregation = req.get_param("aggregation", default="mean")
        if (aggregation not in
           archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS):
            msg = "Invalid aggregation value %(agg)s, must be one of %(std)s"
            abort(400, msg % dict(
                agg=aggregation,
                std=archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS))

        start = req.get_param("start")
        if start is not None:
            try:
                start = utils.to_timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        stop = req.get_param("stop")
        if stop is not None:
            try:
                stop = utils.to_timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        granularity = req.get_param("granularity")
        if granularity is not None:
            try:
                granularity = utils.to_timespan(granularity)
            except ValueError:
                abort(400, {"cause": "Attribute value error",
                            "detail": "granularity",
                            "reason": "Invalid granularity"})

        resample = req.get_param("resample")
        if resample:
            if not granularity:
                abort(400, 'A granularity must be specified to resample')
            try:
                resample = utils.to_timespan(resample)
            except ValueError as e:
                abort(400, six.text_type(e))

        refresh = req.get_param_as_bool("refresh",  blank_as_true=True)
        if refresh and self.incoming.has_unprocessed(metric):
            try:
                self.storage.refresh_metric(
                    self.indexer, self.incoming, metric,
                    self.operation_timeout)
            except storage.SackLockTimeoutError as e:
                abort(503, six.text_type(e))
        try:
            measures = self.storage.get_measures(
                metric, start, stop, aggregation,
                granularity, resample)
        except (storage.MetricDoesNotExist,
                storage.GranularityDoesNotExist,
                storage.AggregationDoesNotExist) as e:
            abort(404, six.text_type(e))
        resp.body = json.dumps(measures)


def make_app(conf, indexer, storage, incoming,
             not_implemented_middleware):
    app = falcon.API()
    policy_enforcer = policy.Enforcer(conf)
    auth_helper = driver.DriverManager("gnocchi.rest.auth_helper",
                                       conf.api.auth_mode,
                                       invoke_on_load=True).driver
    app.add_route('/',
                  MetricsResource(policy_enforcer,
                                  auth_helper, indexer,
                                  conf.api.max_limit))
    app.add_route('/{metric_id:uuid}',
                  MetricResource(policy_enforcer,
                                 auth_helper, indexer))
    app.add_route('/{metric_id:uuid}/measures',
                  MeasuresResource(policy_enforcer,
                                   auth_helper, indexer, storage, incoming,
                                   conf.api.operation_timeout))
    return app
