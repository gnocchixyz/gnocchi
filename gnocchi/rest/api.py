# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2018 Red Hat, Inc.
# Copyright © 2014-2015 eNovance
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
import functools
import itertools
import operator
import uuid

import jsonpatch
import pecan
from pecan import rest
import pyparsing
import six
from six.moves.urllib import parse as urllib_parse
import tenacity
import tooz
import voluptuous
import werkzeug.http

import gnocchi
from gnocchi import archive_policy
from gnocchi import calendar
from gnocchi import chef
from gnocchi.cli import metricd
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import json
from gnocchi import resource_type
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import processor
from gnocchi import storage
from gnocchi import utils

try:
    from gnocchi.rest.prometheus import remote_pb2
    import snappy
    PROMETHEUS_SUPPORTED = True
except ImportError:
    PROMETHEUS_SUPPORTED = False


ATTRGETTER_GRANULARITY = operator.attrgetter("granularity")


def arg_to_list(value):
    if isinstance(value, list):
        return value
    elif value:
        return [value]
    return []


def abort(status_code, detail=''):
    """Like pecan.abort, but make sure detail is a string."""
    if status_code == 404 and not detail:
        raise RuntimeError("http code 404 must have 'detail' set")
    if isinstance(detail, Exception):
        detail = detail.jsonify()
    return pecan.abort(status_code, detail)


def flatten_dict_to_keypairs(d, separator=':'):
    """Generator that produces sequence of keypairs for nested dictionaries.

    :param d: dictionaries which may be nested
    :param separator: symbol between names
    """
    for name, value in sorted(six.iteritems(d)):
        if isinstance(value, dict):
            for subname, subvalue in flatten_dict_to_keypairs(value,
                                                              separator):
                yield ('%s%s%s' % (name, separator, subname), subvalue)
        else:
            yield name, value


def enforce(rule, target):
    """Return the user and project the request should be limited to.

    :param rule: The rule name
    :param target: The target to enforce on.

    """
    creds = pecan.request.auth_helper.get_auth_info(pecan.request)

    if not isinstance(target, dict):
        if hasattr(target, "jsonify"):
            target = target.jsonify()
        else:
            target = target.__dict__

    # Flatten dict
    target = dict(flatten_dict_to_keypairs(d=target, separator='.'))

    if not pecan.request.policy_enforcer.enforce(rule, target, creds):
        abort(403)


def set_resp_location_hdr(location):
    location = '%s%s' % (pecan.request.script_name, location)
    # NOTE(sileht): according the pep-3333 the headers must be
    # str in py2 and py3 even this is not the same thing in both
    # version
    # see: http://legacy.python.org/dev/peps/pep-3333/#unicode-issues
    if six.PY2 and isinstance(location, six.text_type):
        location = location.encode('utf-8')
    location = urllib_parse.quote(location)
    pecan.response.headers['Location'] = location


def set_resp_link_hdr(marker, *args):
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
    pecan.response.headers.add("Link", '<%s?%s>; rel="next"' %
                               (pecan.request.path_url, params))


def deserialize(expected_content_types=None):
    if expected_content_types is None:
        expected_content_types = ("application/json", )

    mime_type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Content-Type'))
    if mime_type not in expected_content_types:
        abort(415)
    try:
        params = json.load(pecan.request.body_file)
    except Exception as e:
        abort(400, "Unable to decode body: " + six.text_type(e))
    return params


def validate(schema, data, required=True, detailed_exc=False):
    try:
        return voluptuous.Schema(schema, required=required)(data)
    except voluptuous.Invalid as e:
        if detailed_exc:
            abort(400, {"cause": "Attribute value error",
                        "reason": str(e),
                        "detail": e.path})
        else:
            abort(400, "Invalid input: %s" % e)


def deserialize_and_validate(schema, required=True,
                             expected_content_types=None,
                             detailed_exc=False):
    return validate(schema,
                    deserialize(expected_content_types=expected_content_types),
                    required,
                    detailed_exc)


def Timespan(value):
    try:
        return utils.to_timespan(value)
    except ValueError as e:
        raise voluptuous.Invalid(e)


def get_bool_param(name, params, default='false'):
    return strtobool(name, params.get(name, default))


def strtobool(varname, v):
    """Convert a string to a boolean."""
    try:
        return utils.strtobool(v)
    except ValueError as e:
        abort(400, "Unable to parse `%s': %s" % (varname, six.text_type(e)))


RESOURCE_DEFAULT_PAGINATION = [u'revision_start:asc',
                               u'started_at:asc']

METRIC_DEFAULT_PAGINATION = [u'id:asc']


def get_pagination_options(params, default):
    try:
        opts = voluptuous.Schema({
            voluptuous.Required(
                "limit", default=pecan.request.conf.api.max_limit):
            voluptuous.All(voluptuous.Coerce(int),
                           voluptuous.Range(min=1),
                           voluptuous.Clamp(
                               min=1, max=pecan.request.conf.api.max_limit)),
            "marker": six.text_type,
            voluptuous.Required("sort", default=default):
            voluptuous.All(
                voluptuous.Coerce(arg_to_list),
                [six.text_type]),
        }, extra=voluptuous.REMOVE_EXTRA)(params)
    except voluptuous.Invalid as e:
        abort(400, {"cause": "Argument value error",
                    "reason": str(e)})
    opts['sorts'] = opts['sort']
    del opts['sort']
    return opts


ArchivePolicyDefinitionSchema = voluptuous.Schema(
    voluptuous.All([{
        "granularity": Timespan,
        "points": voluptuous.All(
            voluptuous.Coerce(int),
            voluptuous.Range(min=1),
        ),
        "timespan": Timespan,
    }], voluptuous.Length(min=1)),
)


class ArchivePolicyController(rest.RestController):
    def __init__(self, archive_policy):
        self.archive_policy = archive_policy

    @pecan.expose('json')
    def get(self):
        ap = pecan.request.indexer.get_archive_policy(self.archive_policy)
        if ap:
            enforce("get archive policy", ap)
            return ap
        abort(404, six.text_type(
            indexer.NoSuchArchivePolicy(self.archive_policy)))

    @pecan.expose('json')
    def patch(self):
        ap = pecan.request.indexer.get_archive_policy(self.archive_policy)
        if not ap:
            abort(404, six.text_type(
                indexer.NoSuchArchivePolicy(self.archive_policy)))
        enforce("update archive policy", ap)

        body = deserialize_and_validate(voluptuous.Schema({
            voluptuous.Required("definition"): ArchivePolicyDefinitionSchema,
        }))
        # Validate the data
        try:
            ap_items = [archive_policy.ArchivePolicyItem(**item) for item in
                        body['definition']]
        except ValueError as e:
            abort(400, six.text_type(e))

        try:
            return pecan.request.indexer.update_archive_policy(
                self.archive_policy, ap_items)
        except indexer.UnsupportedArchivePolicyChange as e:
            abort(400, six.text_type(e))

    @pecan.expose()
    def delete(self):
        # NOTE(jd) I don't think there's any point in fetching and passing the
        # archive policy here, as the rule is probably checking the actual role
        # of the user, not the content of the AP.
        enforce("delete archive policy", {})
        try:
            pecan.request.indexer.delete_archive_policy(self.archive_policy)
        except indexer.NoSuchArchivePolicy as e:
            abort(404, six.text_type(e))
        except indexer.ArchivePolicyInUse as e:
            abort(400, six.text_type(e))


class ArchivePoliciesController(rest.RestController):
    @pecan.expose()
    def _lookup(self, archive_policy, *remainder):
        return ArchivePolicyController(archive_policy), remainder

    @pecan.expose('json')
    def post(self):
        enforce("create archive policy", {})
        # NOTE(jd): Initialize this one at run-time because we rely on conf
        conf = pecan.request.conf
        valid_agg_methods = list(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS_VALUES
        )
        ArchivePolicySchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("back_window", default=0): voluptuous.All(
                voluptuous.Coerce(int),
                voluptuous.Range(min=0),
            ),
            voluptuous.Required(
                "aggregation_methods",
                default=list(conf.archive_policy.default_aggregation_methods)):
            valid_agg_methods,
            voluptuous.Required("definition"): ArchivePolicyDefinitionSchema,
        })

        body = deserialize_and_validate(ArchivePolicySchema)
        # Validate the data
        try:
            ap = archive_policy.ArchivePolicy.from_dict(body)
        except ValueError as e:
            abort(400, six.text_type(e))
        enforce("create archive policy", ap)
        try:
            ap = pecan.request.indexer.create_archive_policy(ap)
        except indexer.ArchivePolicyAlreadyExists as e:
            abort(409, six.text_type(e))

        location = "/archive_policy/" + ap.name
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy", {})
        return pecan.request.indexer.list_archive_policies()


class ArchivePolicyRulesController(rest.RestController):
    @pecan.expose()
    def _lookup(self, archive_policy_rule, *remainder):
        apr = pecan.request.indexer.get_archive_policy_rule(
            archive_policy_rule
        )
        if apr:
            return ArchivePolicyRuleController(apr), remainder
        abort(404, six.text_type(
            indexer.NoSuchArchivePolicyRule(archive_policy_rule)))

    @pecan.expose('json')
    def post(self):
        enforce("create archive policy rule", {})
        ArchivePolicyRuleSchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("metric_pattern"): six.text_type,
            voluptuous.Required("archive_policy_name"): six.text_type,
            })

        body = deserialize_and_validate(ArchivePolicyRuleSchema)
        enforce("create archive policy rule", body)
        try:
            ap = pecan.request.indexer.create_archive_policy_rule(
                body['name'], body['metric_pattern'],
                body['archive_policy_name']
            )
        except indexer.ArchivePolicyRuleAlreadyExists as e:
            abort(409, six.text_type(e))
        except indexer.NoSuchArchivePolicy as e:
            abort(400, e)

        location = "/archive_policy_rule/" + ap.name
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy rule", {})
        return pecan.request.indexer.list_archive_policy_rules()


class ArchivePolicyRuleController(rest.RestController):
    def __init__(self, archive_policy_rule):
        self.archive_policy_rule = archive_policy_rule

    @pecan.expose('json')
    def get(self):
        enforce("get archive policy rule", self.archive_policy_rule)
        return self.archive_policy_rule

    @pecan.expose('json')
    def patch(self):
        ArchivePolicyRuleSchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            })
        body = deserialize_and_validate(ArchivePolicyRuleSchema)
        enforce("update archive policy rule", {})
        try:
            return pecan.request.indexer.update_archive_policy_rule(
                self.archive_policy_rule.name, body["name"])
        except indexer.UnsupportedArchivePolicyRuleChange as e:
            abort(400, six.text_type(e))

    @pecan.expose()
    def delete(self):
        # NOTE(jd) I don't think there's any point in fetching and passing the
        # archive policy rule here, as the rule is probably checking the actual
        # role of the user, not the content of the AP rule.
        enforce("delete archive policy rule", {})
        try:
            pecan.request.indexer.delete_archive_policy_rule(
                self.archive_policy_rule.name
            )
        except indexer.NoSuchArchivePolicyRule as e:
            abort(404, six.text_type(e))


def MeasuresListSchema(measures):
    try:
        times = utils.to_timestamps([m['timestamp'] for m in measures])
    except TypeError:
        raise voluptuous.Invalid("unexpected measures format")
    except ValueError as e:
        raise voluptuous.Invalid("unexpected timestamp '%s'" % e)

    try:
        values = [float(i['value']) for i in measures]
    except Exception:
        raise voluptuous.Invalid("unexpected measures value")

    return (incoming.Measure(t, v) for t, v in six.moves.zip(times, values))


class MetricController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, metric):
        self.metric = metric

    def enforce_metric(self, rule):
        enforce(rule, json.to_primitive(self.metric))

    @pecan.expose('json')
    def get_all(self):
        self.enforce_metric("get metric")
        return self.metric

    @pecan.expose('json')
    def post_measures(self):
        self.enforce_metric("post measures")
        measures = deserialize_and_validate(MeasuresListSchema,
                                            detailed_exc=True)
        if measures:
            pecan.request.incoming.add_measures(self.metric.id, measures)
        pecan.response.status = 202

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean',
                     granularity=None, resample=None, refresh=False,
                     **param):
        self.enforce_metric("get measures")

        if resample:
            if not granularity:
                abort(400, 'A granularity must be specified to resample')
            try:
                resample = (resample if calendar.GROUPINGS.get(resample) else
                            utils.to_timespan(resample))
            except ValueError as e:
                abort(400, six.text_type(e))

        if granularity is None:
            granularity = [d.granularity
                           for d in self.metric.archive_policy.definition]
            start, stop, _, _, _ = validate_qs(
                start=start, stop=stop)
        else:
            start, stop, granularity, _, _ = validate_qs(
                start=start, stop=stop, granularity=granularity)

        if aggregation not in self.metric.archive_policy.aggregation_methods:
            abort(404, {
                "cause": "Aggregation method does not exist for this metric",
                "detail": {
                    "metric": self.metric.id,
                    "aggregation_method": aggregation,
                },
            })

        aggregations = []
        for g in sorted(granularity, reverse=True):
            agg = self.metric.archive_policy.get_aggregation(
                aggregation, g)
            if agg is None:
                abort(404, six.text_type(
                    storage.AggregationDoesNotExist(
                        self.metric, aggregation, g)
                ))
            aggregations.append(agg)

        if (strtobool("refresh", refresh) and
                pecan.request.incoming.has_unprocessed(self.metric.id)):
            try:
                pecan.request.chef.refresh_metrics(
                    [self.metric],
                    pecan.request.conf.api.operation_timeout)
            except chef.SackAlreadyLocked:
                abort(503, 'Unable to refresh metric: %s. Metric is locked. '
                      'Please try again.' % self.metric.id)
        try:
            return pecan.request.storage.get_measures(
                self.metric, aggregations, start, stop, resample)[aggregation]
        except storage.AggregationDoesNotExist as e:
            abort(404, six.text_type(e))
        except storage.MetricDoesNotExist:
            return []

    @pecan.expose()
    def delete(self):
        self.enforce_metric("delete metric")
        try:
            pecan.request.indexer.delete_metric(self.metric.id)
        except indexer.NoSuchMetric as e:
            abort(404, six.text_type(e))


class MetricsController(rest.RestController):

    @pecan.expose()
    def _lookup(self, id, *remainder):
        try:
            metric_id = uuid.UUID(id)
        except ValueError:
            abort(404, six.text_type(indexer.NoSuchMetric(id)))

        # Load details for ACL
        metrics = pecan.request.indexer.list_metrics(
            attribute_filter={"=": {"id": metric_id}}, details=True)
        if not metrics:
            abort(404, six.text_type(indexer.NoSuchMetric(id)))
        return MetricController(metrics[0]), remainder

    # NOTE(jd) Define this method as it was a voluptuous schema – it's just a
    # smarter version of a voluptuous schema, no?
    @staticmethod
    def MetricSchema(definition):
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)

        # First basic validation
        schema = voluptuous.Schema({
            "archive_policy_name": six.text_type,
            "resource_id": functools.partial(ResourceID, creator=creator),
            "name": six.text_type,
            voluptuous.Optional("unit"):
            voluptuous.All(six.text_type, voluptuous.Length(max=31)),
        })
        definition = schema(definition)
        archive_policy_name = definition.get('archive_policy_name')

        name = definition.get('name')
        if name and '/' in name:
            abort(400, "'/' is not supported in metric name")
        if archive_policy_name is None:
            try:
                ap = pecan.request.indexer.get_archive_policy_for_metric(name)
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

        enforce("create metric", {
            "creator": creator,
            "archive_policy_name": archive_policy_name,
            "resource_id": resource_id,
            "original_resource_id": original_resource_id,
            "name": name,
            "unit": definition.get('unit'),
        })

        return definition

    @pecan.expose('json')
    def post(self):
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)
        body = deserialize_and_validate(self.MetricSchema)

        resource_id = body.get('resource_id')
        if resource_id is not None:
            resource_id = resource_id[1]

        try:
            m = pecan.request.indexer.create_metric(
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
        set_resp_location_hdr("/metric/" + str(m.id))
        pecan.response.status = 201
        return m

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

    @classmethod
    @pecan.expose('json')
    def get_all(cls, **kwargs):
        filtering = cls.MetricListSchema(kwargs)

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

        pagination_opts = get_pagination_options(kwargs,
                                                 METRIC_DEFAULT_PAGINATION)
        attr_filters = []
        if provided_creator is not None:
            attr_filters.append({"=": {"creator": provided_creator}})

        for k, v in six.iteritems(filtering):
            attr_filters.append({"=": {k: v}})

        policy_filter = pecan.request.auth_helper.get_metric_policy_filter(
            pecan.request, "list metric")
        resource_policy_filter = (
            pecan.request.auth_helper.get_resource_policy_filter(
                pecan.request, "list metric", resource_type=None,
                prefix="resource")
        )

        try:
            metrics = pecan.request.indexer.list_metrics(
                attribute_filter={"and": attr_filters},
                policy_filter=policy_filter,
                resource_policy_filter=resource_policy_filter,
                **pagination_opts)
            if metrics and len(metrics) >= pagination_opts['limit']:
                set_resp_link_hdr(str(metrics[-1].id), kwargs, pagination_opts)
            return metrics
        except indexer.InvalidPagination as e:
            abort(400, six.text_type(e))


_MetricsSchema = voluptuous.Schema({
    six.text_type: voluptuous.Any(utils.UUID,
                                  MetricsController.MetricSchema),
})


def MetricsSchema(data):
    # NOTE(jd) Before doing any kind of validation, copy the metric name
    # into the metric definition. This is required so we have the name
    # available when doing the metric validation with its own MetricSchema,
    # and so we can do things such as applying archive policy rules.
    if isinstance(data, dict):
        for metric_name, metric_def in six.iteritems(data):
            if isinstance(metric_def, dict):
                metric_def['name'] = metric_name
    return _MetricsSchema(data)


class NamedMetricController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose()
    def _lookup(self, name, *remainder):
        m = pecan.request.indexer.list_metrics(
            details=True,
            attribute_filter={"and": [
                {"=": {"name": name}},
                {"=": {"resource_id": self.resource_id}},
            ]})
        if m:
            return MetricController(m[0]), remainder

        resource = pecan.request.indexer.get_resource(self.resource_type,
                                                      self.resource_id)
        if resource:
            abort(404, six.text_type(indexer.NoSuchMetric(name)))
        else:
            abort(404, six.text_type(indexer.NoSuchResource(self.resource_id)))

    @pecan.expose('json')
    def post(self):
        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, six.text_type(indexer.NoSuchResource(self.resource_id)))
        enforce("update resource", resource)
        metrics = deserialize_and_validate(MetricsSchema)
        try:
            r = pecan.request.indexer.update_resource(
                self.resource_type,
                self.resource_id,
                metrics=metrics,
                append_metrics=True,
                create_revision=False)
        except (indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy,
                ValueError) as e:
            abort(400, six.text_type(e))
        except indexer.NamedMetricAlreadyExists as e:
            abort(409, six.text_type(e))
        except indexer.NoSuchResource as e:
            abort(404, six.text_type(e))

        return r.metrics

    @pecan.expose('json')
    def get_all(self):
        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, six.text_type(indexer.NoSuchResource(self.resource_id)))
        enforce("get resource", resource)
        return pecan.request.indexer.list_metrics(
            attribute_filter={"=": {"resource_id": self.resource_id}})


class ResourceHistoryController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose('json')
    def get(self, **kwargs):
        details = get_bool_param('details', kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)

        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        if not resource:
            abort(404, six.text_type(indexer.NoSuchResource(self.resource_id)))

        enforce("get resource", resource)

        try:
            resources = pecan.request.indexer.list_resources(
                self.resource_type,
                attribute_filter={"=": {"id": self.resource_id}},
                details=details,
                history=True,
                **pagination_opts
            )
            if resources and len(resources) >= pagination_opts['limit']:
                marker = "%s@%s" % (resources[-1].id, resources[-1].revision)
                set_resp_link_hdr(marker, kwargs, pagination_opts)
            return resources
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))


def etag_precondition_check(obj):
    etag, lastmodified = obj.etag, obj.lastmodified
    # NOTE(sileht): Checks and order come from rfc7232
    # in webob, the '*' and the absent of the header is handled by
    # if_match.__contains__() and if_none_match.__contains__()
    # and are identique...
    if etag not in pecan.request.if_match:
        abort(412)
    elif (not pecan.request.environ.get("HTTP_IF_MATCH")
          and pecan.request.if_unmodified_since
          and pecan.request.if_unmodified_since < lastmodified):
        abort(412)

    if etag in pecan.request.if_none_match:
        if pecan.request.method in ['GET', 'HEAD']:
            abort(304)
        else:
            abort(412)
    elif (not pecan.request.environ.get("HTTP_IF_NONE_MATCH")
          and pecan.request.if_modified_since
          and (pecan.request.if_modified_since >=
               lastmodified)
          and pecan.request.method in ['GET', 'HEAD']):
        abort(304)


def etag_set_headers(obj):
    pecan.response.etag = obj.etag
    pecan.response.last_modified = obj.lastmodified


def AttributesPath(value):
    if value.startswith("/attributes"):
        return value
    raise ValueError("Only attributes can be modified")


ResourceTypeJsonPatchSchema = voluptuous.Schema([{
    "op": voluptuous.Any("add", "remove"),
    "path": AttributesPath,
    voluptuous.Optional("value"): dict,
}])


class ResourceTypeController(rest.RestController):
    def __init__(self, name):
        self._name = name

    @pecan.expose('json')
    def get(self):
        try:
            rt = pecan.request.indexer.get_resource_type(self._name)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        enforce("get resource type", rt)
        return rt

    @pecan.expose('json')
    def patch(self):
        # NOTE(sileht): should we check for "application/json-patch+json"
        # Content-Type ?

        try:
            rt = pecan.request.indexer.get_resource_type(self._name)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        enforce("update resource type", rt)

        # Ensure this is a valid jsonpatch dict
        patch = deserialize_and_validate(
            ResourceTypeJsonPatchSchema,
            expected_content_types=["application/json-patch+json"])

        # Add new attributes to the resource type
        rt_json_current = rt.jsonify()
        try:
            rt_json_next = jsonpatch.apply_patch(rt_json_current, patch)
        except jsonpatch.JsonPatchException as e:
            abort(400, six.text_type(e))
        del rt_json_next['state']

        # Validate that the whole new resource_type is valid
        schema = pecan.request.indexer.get_resource_type_schema()
        try:
            rt_json_next = voluptuous.Schema(schema.for_update, required=True)(
                rt_json_next)
        except voluptuous.Error as e:
            abort(400, "Invalid input: %s" % e)

        # Get only newly formatted and deleted attributes
        add_attrs = {k: v for k, v in rt_json_next["attributes"].items()
                     if k not in rt_json_current["attributes"]}
        del_attrs = [k for k in rt_json_current["attributes"]
                     if k not in rt_json_next["attributes"]]

        if not add_attrs and not del_attrs:
            # NOTE(sileht): just returns the resource, the asked changes
            # just do nothing
            return rt

        try:
            add_attrs = schema.attributes_from_dict(add_attrs)
        except resource_type.InvalidResourceAttribute as e:
            abort(400, "Invalid input: %s" % e)

        try:
            return pecan.request.indexer.update_resource_type(
                self._name, add_attributes=add_attrs,
                del_attributes=del_attrs)
        except indexer.NoSuchResourceType as e:
                abort(400, six.text_type(e))

    @pecan.expose()
    def delete(self):
        try:
            pecan.request.indexer.get_resource_type(self._name)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        enforce("delete resource type", resource_type)
        try:
            pecan.request.indexer.delete_resource_type(self._name)
        except (indexer.NoSuchResourceType,
                indexer.ResourceTypeInUse) as e:
            abort(400, six.text_type(e))


class ResourceTypesController(rest.RestController):

    @pecan.expose()
    def _lookup(self, name, *remainder):
        return ResourceTypeController(name), remainder

    @pecan.expose('json')
    def post(self):
        schema = pecan.request.indexer.get_resource_type_schema()
        body = deserialize_and_validate(schema)
        body["state"] = "creating"

        try:
            rt = schema.resource_type_from_dict(**body)
        except resource_type.InvalidResourceAttribute as e:
            abort(400, "Invalid input: %s" % e)

        enforce("create resource type", body)
        try:
            rt = pecan.request.indexer.create_resource_type(rt)
        except indexer.ResourceTypeAlreadyExists as e:
            abort(409, six.text_type(e))
        set_resp_location_hdr("/resource_type/" + rt.name)
        pecan.response.status = 201
        return rt

    @pecan.expose('json')
    def get_all(self, **kwargs):
        enforce("list resource type", {})
        try:
            return pecan.request.indexer.list_resource_types()
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))


def ResourceSchema(schema):
    base_schema = {
        voluptuous.Optional('started_at'): utils.to_datetime,
        voluptuous.Optional('ended_at'): utils.to_datetime,
        voluptuous.Optional('user_id'): voluptuous.Any(None, six.text_type),
        voluptuous.Optional('project_id'): voluptuous.Any(None, six.text_type),
        voluptuous.Optional('metrics'): MetricsSchema,
    }
    base_schema.update(schema)
    return base_schema


class ResourceController(rest.RestController):

    def __init__(self, resource_type, id):
        self._resource_type = resource_type
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)
        try:
            self.id = utils.ResourceUUID(id, creator)
        except ValueError:
            abort(404, six.text_type(indexer.NoSuchResource(id)))
        self.metric = NamedMetricController(str(self.id), self._resource_type)
        self.history = ResourceHistoryController(str(self.id),
                                                 self._resource_type)

    @pecan.expose('json')
    def get(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id, with_metrics=True)
        if resource:
            enforce("get resource", resource)
            etag_precondition_check(resource)
            etag_set_headers(resource)
            return resource
        abort(404, six.text_type(indexer.NoSuchResource(self.id)))

    @pecan.expose('json')
    def patch(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id, with_metrics=True)
        if not resource:
            abort(404, six.text_type(indexer.NoSuchResource(self.id)))
        enforce("update resource", resource)
        etag_precondition_check(resource)

        body = deserialize_and_validate(
            schema_for(self._resource_type),
            required=False)

        if len(body) == 0:
            etag_set_headers(resource)
            return resource

        for k, v in six.iteritems(body):
            if k != 'metrics' and getattr(resource, k) != v:
                create_revision = True
                break
        else:
            if 'metrics' not in body:
                # No need to go further, we assume the db resource
                # doesn't change between the get and update
                return resource
            create_revision = False

        try:
            resource = pecan.request.indexer.update_resource(
                self._resource_type,
                self.id,
                create_revision=create_revision,
                **body)
        except (indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy,
                ValueError) as e:
            abort(400, six.text_type(e))
        except indexer.NoSuchResource as e:
            abort(404, six.text_type(e))
        etag_set_headers(resource)
        return resource

    @pecan.expose()
    def delete(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if not resource:
            abort(404, six.text_type(indexer.NoSuchResource(self.id)))
        enforce("delete resource", resource)
        etag_precondition_check(resource)
        try:
            pecan.request.indexer.delete_resource(self.id)
        except indexer.NoSuchResource as e:
            abort(404, six.text_type(e))


def schema_for(resource_type):
    resource_type = pecan.request.indexer.get_resource_type(resource_type)
    return ResourceSchema(resource_type.schema)


def ResourceUUID(value, creator):
    try:
        return utils.ResourceUUID(value, creator)
    except ValueError as e:
        raise voluptuous.Invalid(e)


def ResourceID(value, creator):
    """Convert value to a resource ID.

    :return: A tuple (original_resource_id, resource_id)
    """
    return (six.text_type(value), ResourceUUID(value, creator))


class ResourcesController(rest.RestController):
    def __init__(self, resource_type):
        self._resource_type = resource_type

    @pecan.expose()
    def _lookup(self, id, *remainder):
        return ResourceController(self._resource_type, id), remainder

    @pecan.expose('json')
    def post(self):
        # NOTE(sileht): we need to copy the dict because when change it
        # and we don't want that next patch call have the "id"
        schema = dict(schema_for(self._resource_type))
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)
        schema["id"] = functools.partial(ResourceID, creator=creator)

        body = deserialize_and_validate(schema)
        body["original_resource_id"], body["id"] = body["id"]

        target = {
            "resource_type": self._resource_type,
        }
        target.update(body)
        enforce("create resource", target)
        rid = body['id']
        del body['id']
        try:
            resource = pecan.request.indexer.create_resource(
                self._resource_type, rid, creator,
                **body)
        except (ValueError,
                indexer.NoSuchMetric,
                indexer.NoSuchArchivePolicy) as e:
            abort(400, six.text_type(e))
        except indexer.ResourceAlreadyExists as e:
            abort(409, six.text_type(e))
        set_resp_location_hdr("/resource/"
                              + self._resource_type + "/"
                              + six.text_type(resource.id))
        etag_set_headers(resource)
        pecan.response.status = 201
        return resource

    @pecan.expose('json')
    def get_all(self, **kwargs):
        details = get_bool_param('details', kwargs)
        history = get_bool_param('history', kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)
        json_attrs = arg_to_list(kwargs.get('attrs', None))
        policy_filter = pecan.request.auth_helper.get_resource_policy_filter(
            pecan.request, "list resource", self._resource_type)

        try:
            # FIXME(sileht): next API version should returns
            # {'resources': [...], 'links': [ ... pagination rel ...]}
            resources = pecan.request.indexer.list_resources(
                self._resource_type,
                attribute_filter=policy_filter,
                details=details,
                history=history,
                **pagination_opts
            )
            if resources and len(resources) >= pagination_opts['limit']:
                if history:
                    marker = "%s@%s" % (resources[-1].id,
                                        resources[-1].revision)
                else:
                    marker = str(resources[-1].id)
                set_resp_link_hdr(marker, kwargs, pagination_opts)
            return [r.jsonify(json_attrs) for r in resources]
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))

    @pecan.expose('json')
    def delete(self, **kwargs):
        # NOTE(sileht): Don't allow empty filter, this is going to delete
        # the entire database.
        if pecan.request.body:
            attr_filter = deserialize_and_validate(ResourceSearchSchema)
        elif kwargs.get("filter"):
            attr_filter = QueryStringSearchAttrFilter.parse(kwargs["filter"])
        else:
            attr_filter = None

        # the voluptuous checks everything, but it is better to
        # have this here.
        if not attr_filter:
            abort(400, "caution: the query can not be empty, or it will \
                  delete entire database")

        policy_filter = pecan.request.auth_helper.get_resource_policy_filter(
            pecan.request,
            "delete resources", self._resource_type)

        if policy_filter:
            attr_filter = {"and": [policy_filter, attr_filter]}

        try:
            delete_num = pecan.request.indexer.delete_resources(
                self._resource_type, attribute_filter=attr_filter)
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))

        return {"deleted": delete_num}


class ResourcesByTypeController(rest.RestController):
    @pecan.expose('json')
    def get_all(self):
        return dict(
            (rt.name,
             pecan.request.application_url + '/resource/' + rt.name)
            for rt in pecan.request.indexer.list_resource_types())

    @pecan.expose()
    def _lookup(self, resource_type, *remainder):
        try:
            pecan.request.indexer.get_resource_type(resource_type)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        return ResourcesController(resource_type), remainder


class QueryStringSearchAttrFilter(object):
    uninary_operators = ("not", )
    binary_operator = (u">=", u"<=", u"!=", u">", u"<", u"=", u"==", u"eq",
                       u"ne", u"lt", u"gt", u"ge", u"le", u"in", u"like", u"≠",
                       u"≥", u"≤")
    multiple_operators = (u"and", u"or", u"∧", u"∨")

    operator = pyparsing.Regex(u"|".join(binary_operator))
    null = pyparsing.Regex("None|none|null").setParseAction(
        pyparsing.replaceWith(None))
    boolean = "False|True|false|true"
    boolean = pyparsing.Regex(boolean).setParseAction(
        lambda t: t[0].lower() == "true")
    hex_string = lambda n: pyparsing.Word(pyparsing.hexnums, exact=n)
    uuid_string = pyparsing.Combine(
        hex_string(8) + (pyparsing.Optional("-") + hex_string(4)) * 3 +
        pyparsing.Optional("-") + hex_string(12))
    number = r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?"
    number = pyparsing.Regex(number).setParseAction(lambda t: float(t[0]))
    identifier = pyparsing.Word(pyparsing.alphas, pyparsing.alphanums + "_")
    quoted_string = pyparsing.QuotedString('"') | pyparsing.QuotedString("'")
    comparison_term = pyparsing.Forward()
    in_list = pyparsing.Group(
        pyparsing.Suppress('[') +
        pyparsing.Optional(pyparsing.delimitedList(comparison_term)) +
        pyparsing.Suppress(']'))("list")
    comparison_term << (null | boolean | uuid_string | identifier | number |
                        quoted_string | in_list)
    condition = pyparsing.Group(comparison_term + operator + comparison_term)

    expr = pyparsing.infixNotation(condition, [
        ("not", 1, pyparsing.opAssoc.RIGHT, ),
        ("and", 2, pyparsing.opAssoc.LEFT, ),
        ("∧", 2, pyparsing.opAssoc.LEFT, ),
        ("or", 2, pyparsing.opAssoc.LEFT, ),
        ("∨", 2, pyparsing.opAssoc.LEFT, ),
    ])

    @classmethod
    def _parsed_query2dict(cls, parsed_query):
        result = None
        while parsed_query:
            part = parsed_query.pop()
            if part in cls.binary_operator:
                result = {part: {parsed_query.pop(): result}}

            elif part in cls.multiple_operators:
                if result.get(part):
                    result[part].append(
                        cls._parsed_query2dict(parsed_query.pop()))
                else:
                    result = {part: [result]}

            elif part in cls.uninary_operators:
                result = {part: result}
            elif isinstance(part, pyparsing.ParseResults):
                kind = part.getName()
                if kind == "list":
                    res = part.asList()
                else:
                    res = cls._parsed_query2dict(part)
                if result is None:
                    result = res
                elif isinstance(result, dict):
                    list(result.values())[0].append(res)
            else:
                result = part
        return result

    @classmethod
    def _parse(cls, query):
        try:
            parsed_query = cls.expr.parseString(query, parseAll=True)[0]
        except pyparsing.ParseException as e:
            raise abort(400, "Invalid filter: %s" % str(e))
        return cls._parsed_query2dict(parsed_query)

    @classmethod
    def parse(cls, query):
        attr_filter = cls._parse(query)
        return validate(ResourceSearchSchema, attr_filter, required=True)


def ResourceSearchSchema(v):
    return _ResourceSearchSchema()(v)


# NOTE(sileht): indexer will cast this type to the real attribute
# type, here we just want to be sure this is not a dict or a list
ResourceSearchSchemaAttributeValue = voluptuous.Any(
    six.text_type, float, int, bool, None)


NotIDKey = voluptuous.All(six.text_type, voluptuous.NotIn(["id"]))


def _ResourceSearchSchema():
    user = pecan.request.auth_helper.get_current_user(
        pecan.request)
    _ResourceUUID = functools.partial(ResourceUUID, creator=user)

    return voluptuous.Schema(
        voluptuous.All(
            voluptuous.Length(min=0, max=1),
            {
                voluptuous.Any(
                    u"=", u"==", u"eq",
                    u"<", u"lt",
                    u">", u"gt",
                    u"<=", u"≤", u"le",
                    u">=", u"≥", u"ge",
                    u"!=", u"≠", u"ne",
                ): voluptuous.All(
                    voluptuous.Length(min=1, max=1),
                    {"id": _ResourceUUID,
                     NotIDKey: ResourceSearchSchemaAttributeValue},
                ),
                u"like": voluptuous.All(
                    voluptuous.Length(min=1, max=1),
                    {NotIDKey: ResourceSearchSchemaAttributeValue},
                ),
                u"in": voluptuous.All(
                    voluptuous.Length(min=1, max=1),
                    {"id": voluptuous.All(
                        [_ResourceUUID],
                        voluptuous.Length(min=1)),
                     NotIDKey: voluptuous.All(
                         [ResourceSearchSchemaAttributeValue],
                         voluptuous.Length(min=1))}
                ),
                voluptuous.Any(
                    u"and", u"∨",
                    u"or", u"∧",
                ): voluptuous.All(
                    [ResourceSearchSchema], voluptuous.Length(min=1)
                ),
                u"not": ResourceSearchSchema,
            }
        )
    )


class SearchResourceTypeController(rest.RestController):
    def __init__(self, resource_type):
        self._resource_type = resource_type

    def _search(self, **kwargs):
        if pecan.request.body:
            attr_filter = deserialize_and_validate(ResourceSearchSchema)
        elif kwargs.get("filter"):
            attr_filter = QueryStringSearchAttrFilter.parse(kwargs["filter"])
        else:
            attr_filter = None

        details = get_bool_param('details', kwargs)
        history = get_bool_param('history', kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)

        policy_filter = pecan.request.auth_helper.get_resource_policy_filter(
            pecan.request, "search resource", self._resource_type)
        if policy_filter:
            if attr_filter:
                attr_filter = {"and": [
                    policy_filter,
                    attr_filter
                ]}
            else:
                attr_filter = policy_filter

        resources = pecan.request.indexer.list_resources(
            self._resource_type,
            attribute_filter=attr_filter,
            details=details,
            history=history,
            **pagination_opts)
        if resources and len(resources) >= pagination_opts['limit']:
            if history:
                marker = "%s@%s" % (resources[-1].id,
                                    resources[-1].revision)
            else:
                marker = str(resources[-1].id)
            set_resp_link_hdr(marker, kwargs, pagination_opts)
        return resources

    @pecan.expose('json')
    def post(self, **kwargs):
        json_attrs = arg_to_list(kwargs.get('attrs', None))
        try:
            return [r.jsonify(json_attrs) for r in self._search(**kwargs)]
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))


class SearchResourceController(rest.RestController):
    @pecan.expose()
    def _lookup(self, resource_type, *remainder):
        try:
            pecan.request.indexer.get_resource_type(resource_type)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        return SearchResourceTypeController(resource_type), remainder


def _MetricSearchSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchMetricController.MetricSearchSchema(v)


def _MetricSearchOperationSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchMetricController.MetricSearchOperationSchema(v)


class SearchMetricController(rest.RestController):

    MetricSearchOperationSchema = voluptuous.Schema(
        voluptuous.All(
            voluptuous.Length(min=1, max=1),
            {
                voluptuous.Any(
                    u"=", u"==", u"eq",
                    u"<", u"lt",
                    u">", u"gt",
                    u"<=", u"≤", u"le",
                    u">=", u"≥", u"ge",
                    u"!=", u"≠", u"ne",
                    u"%", u"mod",
                    u"+", u"add",
                    u"-", u"sub",
                    u"*", u"×", u"mul",
                    u"/", u"÷", u"div",
                    u"**", u"^", u"pow",
                ): voluptuous.Any(
                    float, int,
                    voluptuous.All(
                        [float, int,
                         voluptuous.Any(_MetricSearchOperationSchema)],
                        voluptuous.Length(min=2, max=2),
                    ),
                ),
            },
        )
    )

    MetricSearchSchema = voluptuous.Schema(
        voluptuous.Any(
            MetricSearchOperationSchema,
            voluptuous.All(
                voluptuous.Length(min=1, max=1),
                {
                    voluptuous.Any(
                        u"and", u"∨",
                        u"or", u"∧",
                        u"not",
                    ): [_MetricSearchSchema],
                }
            )
        )
    )

    class MeasureQuery(object):
        binary_operators = {
            u"=": operator.eq,
            u"==": operator.eq,
            u"eq": operator.eq,

            u"<": operator.lt,
            u"lt": operator.lt,

            u">": operator.gt,
            u"gt": operator.gt,

            u"<=": operator.le,
            u"≤": operator.le,
            u"le": operator.le,

            u">=": operator.ge,
            u"≥": operator.ge,
            u"ge": operator.ge,

            u"!=": operator.ne,
            u"≠": operator.ne,
            u"ne": operator.ne,

            u"%": operator.mod,
            u"mod": operator.mod,

            u"+": operator.add,
            u"add": operator.add,

            u"-": operator.sub,
            u"sub": operator.sub,

            u"*": operator.mul,
            u"×": operator.mul,
            u"mul": operator.mul,

            u"/": operator.truediv,
            u"÷": operator.truediv,
            u"div": operator.truediv,

            u"**": operator.pow,
            u"^": operator.pow,
            u"pow": operator.pow,
        }

        multiple_operators = {
            u"or": any,
            u"∨": any,
            u"and": all,
            u"∧": all,
        }

        def __init__(self, tree):
            self._eval = self.build_evaluator(tree)

        def __call__(self, value):
            return self._eval(value)

        def build_evaluator(self, tree):
            try:
                operator, nodes = list(tree.items())[0]
            except Exception:
                return lambda value: tree
            try:
                op = self.multiple_operators[operator]
            except KeyError:
                try:
                    op = self.binary_operators[operator]
                except KeyError:
                    raise self.InvalidQuery("Unknown operator %s" % operator)
                return self._handle_binary_op(op, nodes)
            return self._handle_multiple_op(op, nodes)

        def _handle_multiple_op(self, op, nodes):
            elements = [self.build_evaluator(node) for node in nodes]
            return lambda value: op((e(value) for e in elements))

        def _handle_binary_op(self, op, node):
            try:
                iterator = iter(node)
            except Exception:
                return lambda value: op(value, node)
            nodes = list(iterator)
            if len(nodes) != 2:
                raise self.InvalidQuery(
                    "Binary operator %s needs 2 arguments, %d given" %
                    (op, len(nodes)))
            node0 = self.build_evaluator(node[0])
            node1 = self.build_evaluator(node[1])
            return lambda value: op(node0(value), node1(value))

        class InvalidQuery(Exception):
            pass

    @pecan.expose('json')
    def post(self, metric_id, start=None, stop=None, aggregation='mean',
             granularity=None):
        metrics = pecan.request.indexer.list_metrics(
            attribute_filter={"in": {"id": arg_to_list(metric_id)}})

        for metric in metrics:
            enforce("search metric", metric)

        if not pecan.request.body:
            abort(400, "No query specified in body")

        query = deserialize_and_validate(self.MetricSearchSchema)

        if start is not None:
            try:
                start = utils.to_timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = utils.to_timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        try:
            predicate = self.MeasureQuery(query)
        except self.MeasureQuery.InvalidQuery as e:
            abort(400, six.text_type(e))

        if granularity is not None:
            granularity = sorted(
                map(utils.to_timespan, arg_to_list(granularity)),
                reverse=True)

        metrics_and_aggregations = collections.defaultdict(list)

        for metric in metrics:
            if granularity is None:
                granularity = sorted((
                    d.granularity
                    for d in metric.archive_policy.definition),
                    reverse=True)
            for gr in granularity:
                agg = metric.archive_policy.get_aggregation(
                    aggregation, gr)
                if agg is None:
                    abort(400,
                          storage.AggregationDoesNotExist(
                              metric, aggregation, gr))
                metrics_and_aggregations[metric].append(agg)

        try:
            timeseries = pecan.request.storage.get_aggregated_measures(
                metrics_and_aggregations, start, stop)
        except storage.MetricDoesNotExist as e:
            # This can happen if all the metrics have been created but one
            # doesn't have any measures yet.
            abort(400, e)

        return {
            str(metric.id): [
                (timestamp, aggregation.granularity, value)
                for aggregation, ts in six.iteritems(aggregations_and_ts)
                for timestamp, value in ts
                if predicate(value)
            ]
            for metric, aggregations_and_ts in six.iteritems(timeseries)
        }


class ResourcesMetricsMeasuresBatchController(rest.RestController):

    @staticmethod
    def BackwardCompatibleMeasuresList(v):
        v = voluptuous.Schema(
            voluptuous.Any(MeasuresListSchema,
                           {voluptuous.Optional("archive_policy_name"):
                            six.text_type,
                            voluptuous.Optional("unit"):
                            six.text_type,
                            "measures": MeasuresListSchema}),
            required=True)(v)
        if isinstance(v, dict):
            return v
        else:
            # Old format
            return {"measures": v}

    @pecan.expose('json')
    def post(self, create_metrics=False):
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)
        MeasuresBatchSchema = voluptuous.Schema(
            {functools.partial(ResourceID, creator=creator):
             {six.text_type: self.BackwardCompatibleMeasuresList}})
        body = deserialize_and_validate(MeasuresBatchSchema,
                                        detailed_exc=True)

        known_metrics = []
        unknown_metrics = []
        unknown_resources = []
        body_by_rid = {}

        attribute_filter = {"or": []}
        for original_resource_id, resource_id in body:
            names = list(body[(original_resource_id, resource_id)].keys())
            if names:
                attribute_filter["or"].append({"and": [
                    {"=": {"resource_id": resource_id}},
                    {"in": {"name": names}}]})

        if not attribute_filter["or"]:
            pecan.response.status = 202
            return

        all_metrics = collections.defaultdict(list)
        for metric in pecan.request.indexer.list_metrics(
                attribute_filter=attribute_filter):
            all_metrics[metric.resource_id].append(metric)

        for original_resource_id, resource_id in body:
            r = body[(original_resource_id, resource_id)]
            body_by_rid[resource_id] = r
            names = list(r.keys())
            metrics = all_metrics[resource_id]

            known_names = [m.name for m in metrics]
            if strtobool("create_metrics", create_metrics):
                already_exists_names = []
                for name in names:
                    if name not in known_names:
                        metric_data = {"name": name}
                        for attr in ["archive_policy_name", "unit"]:
                            if attr in r[name]:
                                metric_data[attr] = r[name][attr]
                        metric = MetricsController.MetricSchema(metric_data)
                        try:
                            m = pecan.request.indexer.create_metric(
                                uuid.uuid4(),
                                creator=creator,
                                resource_id=resource_id,
                                name=metric.get('name'),
                                unit=metric.get('unit'),
                                archive_policy_name=metric[
                                    'archive_policy_name'])
                        except indexer.NamedMetricAlreadyExists as e:
                            already_exists_names.append(e.metric_name)
                        except indexer.NoSuchResource:
                            unknown_resources.append({
                                'resource_id': six.text_type(resource_id),
                                'original_resource_id': original_resource_id})
                            break
                        except indexer.IndexerException as e:
                            # This catch NoSuchArchivePolicy, which is unlikely
                            # be still possible
                            abort(400, six.text_type(e))
                        else:
                            known_metrics.append(m)

                if already_exists_names:
                    # Add metrics created in the meantime
                    known_names.extend(already_exists_names)
                    known_metrics.extend(
                        pecan.request.indexer.list_metrics(
                            attribute_filter={"and": [
                                {"=": {"resource_id": resource_id}},
                                {"in": {"name": already_exists_names}},
                            ]}))

            elif len(names) != len(metrics):
                unknown_metrics.extend(
                    ["%s/%s" % (six.text_type(resource_id), m)
                     for m in names if m not in known_names])

            known_metrics.extend(metrics)

        if unknown_resources:
            abort(400, {"cause": "Unknown resources",
                        "detail": unknown_resources})

        if unknown_metrics:
            abort(400, "Unknown metrics: %s" % ", ".join(
                sorted(unknown_metrics)))

        for metric in known_metrics:
            enforce("post measures", metric)

        pecan.request.incoming.add_measures_batch(
            dict((metric.id,
                 body_by_rid[metric.resource_id][metric.name]["measures"])
                 for metric in known_metrics))

        pecan.response.status = 202


class MetricsMeasuresBatchController(rest.RestController):
    # NOTE(sileht): we don't allow to mix both formats
    # to not have to deal with id collision that can
    # occurs between a metric_id and a resource_id.
    # Because while json allow duplicate keys in dict payload
    # only the last key will be retain by json python module to
    # build the python dict.
    MeasuresBatchSchema = voluptuous.Schema(
        {utils.UUID: MeasuresListSchema}
    )

    @pecan.expose("json")
    def post(self):
        body = deserialize_and_validate(self.MeasuresBatchSchema)
        metrics = pecan.request.indexer.list_metrics(
            attribute_filter={"in": {"id": list(body.keys())}})

        if len(metrics) != len(body):
            missing_metrics = sorted(set(body) - set(m.id for m in metrics))
            abort(400, "Unknown metrics: %s" % ", ".join(
                six.moves.map(str, missing_metrics)))

        for metric in metrics:
            enforce("post measures", metric)

        pecan.request.incoming.add_measures_batch(
            dict((metric.id, body[metric.id]) for metric in
                 metrics))

        pecan.response.status = 202


class SearchController(object):
    resource = SearchResourceController()
    metric = SearchMetricController()


class AggregationResourceController(rest.RestController):
    def __init__(self, resource_type, metric_name):
        self.resource_type = resource_type
        self.metric_name = metric_name

    @pecan.expose('json')
    def post(self, start=None, stop=None, aggregation='mean',
             reaggregation=None, granularity=None, needed_overlap=100.0,
             groupby=None, fill=None, refresh=False, resample=None,
             **kwargs):
        # First, set groupby in the right format: a sorted list of unique
        # strings.
        groupby = sorted(set(arg_to_list(groupby)))

        # NOTE(jd) Sort by groupby so we are sure we do not return multiple
        # groups when using itertools.groupby later.
        try:
            resources = SearchResourceTypeController(
                self.resource_type)._search(sort=groupby,
                                            filter=kwargs.get("filter"))
        except indexer.InvalidPagination:
            abort(400, "Invalid groupby attribute")
        except indexer.IndexerException as e:
            abort(400, six.text_type(e))

        if resources is None:
            return []

        if not groupby:
            metrics = list(filter(None,
                                  (r.get_metric(self.metric_name)
                                   for r in resources)))
            return AggregationController.get_cross_metric_measures_from_objs(
                metrics, start, stop, aggregation, reaggregation,
                granularity, needed_overlap, fill, refresh, resample)

        def groupper(r):
            return tuple((attr, r[attr]) for attr in groupby)

        results = []
        for key, resources in itertools.groupby(resources, groupper):
            metrics = list(filter(None,
                                  (r.get_metric(self.metric_name)
                                   for r in resources)))
            results.append({
                "group": dict(key),
                "measures": AggregationController.get_cross_metric_measures_from_objs(  # noqa
                    metrics, start, stop, aggregation, reaggregation,
                    granularity, needed_overlap, fill, refresh, resample)
            })

        return results

FillSchema = voluptuous.Schema(
    voluptuous.Any(voluptuous.Coerce(float), "null", "dropna",
                   msg="Must be a float, 'dropna' or 'null'"))


def validate_qs(start=None, stop=None, granularity=None,
                needed_overlap=None, fill=None):
    if needed_overlap is not None:
        try:
            needed_overlap = float(needed_overlap)
        except ValueError:
            abort(400, {"cause": "Argument value error",
                        "detail": "needed_overlap",
                        "reason": "Must be a number"})

    if start is not None:
        try:
            start = utils.to_timestamp(start)
        except Exception:
            abort(400, {"cause": "Argument value error",
                        "detail": "start",
                        "reason": "Must be a datetime or a timestamp"})

    if stop is not None:
        try:
            stop = utils.to_timestamp(stop)
        except Exception:
            abort(400, {"cause": "Argument value error",
                        "detail": "stop",
                        "reason": "Must be a datetime or a timestamp"})

    if granularity is not None:
        try:
            granularity = [utils.to_timespan(granularity)]
        except ValueError as e:
            abort(400, {"cause": "Argument value error",
                        "detail": "granularity",
                        "reason": six.text_type(e)})

    if fill is not None:
        try:
            fill = FillSchema(fill)
        except voluptuous.Error as e:
            abort(400, {"cause": "Argument value error",
                        "detail": "fill",
                        "reason": str(e)})

    return start, stop, granularity, needed_overlap, fill


class AggregationController(rest.RestController):
    _custom_actions = {
        'metric': ['POST', 'GET'],
    }

    @pecan.expose()
    def _lookup(self, object_type, resource_type, key, metric_name,
                *remainder):
        if object_type != "resource" or key != "metric":
            # NOTE(sileht): we want the raw 404 message here
            # so use directly pecan
            pecan.abort(404)
        try:
            pecan.request.indexer.get_resource_type(resource_type)
        except indexer.NoSuchResourceType as e:
            abort(404, six.text_type(e))
        return AggregationResourceController(resource_type,
                                             metric_name), remainder

    @staticmethod
    def get_cross_metric_measures_from_objs(metrics, start=None, stop=None,
                                            aggregation='mean',
                                            reaggregation=None,
                                            granularity=None,
                                            needed_overlap=100.0, fill=None,
                                            refresh=False, resample=None):
        start, stop, granularity, needed_overlap, fill = validate_qs(
            start, stop, granularity, needed_overlap, fill)

        if reaggregation is None:
            reaggregation = aggregation

        for metric in metrics:
            enforce("get metric", metric)

        number_of_metrics = len(metrics)
        if number_of_metrics == 0:
            return []

        if resample:
            if not granularity:
                abort(400, 'A granularity must be specified to resample')
            try:
                resample = (resample if calendar.GROUPINGS.get(resample) else
                            utils.to_timespan(resample))
            except ValueError as e:
                abort(400, six.text_type(e))

        if granularity is None:
            granularities = (
                definition.granularity
                for m in metrics
                for definition in m.archive_policy.definition
            )
            # granularities_in_common
            granularity = [
                g
                for g, occurrence in six.iteritems(
                    collections.Counter(granularities))
                if occurrence == len(metrics)
            ]

            if not granularity:
                abort(400, exceptions.UnAggregableTimeseries(
                    list((metric.id, aggregation)
                         for metric in metrics),
                    'No granularity match'))

        aggregations = set()
        for metric in metrics:
            for g in granularity:
                agg = metric.archive_policy.get_aggregation(
                    aggregation, g)
                if agg is None:
                    abort(404, six.text_type(
                        storage.AggregationDoesNotExist(metric, aggregation, g)
                    ))
                aggregations.add(agg)
        aggregations = sorted(aggregations, key=ATTRGETTER_GRANULARITY,
                              reverse=True)

        operations = ["aggregate", reaggregation, []]
        if resample:
            operations[2].extend(
                ["resample", aggregation, resample,
                 ["metric"] + [[str(m.id), aggregation]
                               for m in metrics]]
            )
        else:
            operations[2].extend(
                ["metric"] + [[str(m.id), aggregation]
                              for m in metrics]
            )

        try:
            if strtobool("refresh", refresh):
                metrics_to_update = [
                    m for m in metrics
                    if pecan.request.incoming.has_unprocessed(m.id)]
                for m in metrics_to_update:
                    try:
                        pecan.request.chef.refresh_metrics(
                            [m], pecan.request.conf.api.operation_timeout)
                    except chef.SackAlreadyLocked:
                        abort(503, 'Unable to refresh metric: %s. '
                              'Metric is locked. '
                              'Please try again.' % m.id)
            if number_of_metrics == 1:
                # NOTE(sileht): don't do the aggregation if we only have one
                # metric
                metric = metrics[0]
                if (aggregation
                   not in metric.archive_policy.aggregation_methods):
                    abort(404, {
                        "cause":
                        "Aggregation method does not exist for this metric",
                        "detail": {
                            "metric": str(metric.id),
                            "aggregation_method": aggregation,
                        },
                    })
                try:
                    return pecan.request.storage.get_measures(
                        metric, aggregations, start, stop, resample
                    )[aggregation]
                except storage.MetricDoesNotExist:
                    return []
            return processor.get_measures(
                pecan.request.storage,
                [processor.MetricReference(m, aggregation) for m in metrics],
                operations, start, stop,
                granularity, needed_overlap, fill)["aggregated"]
        except exceptions.UnAggregableTimeseries as e:
            abort(400, e)
        except storage.AggregationDoesNotExist as e:
            abort(404, six.text_type(e))

    MetricIDsSchema = [utils.UUID]

    @pecan.expose('json')
    def get_metric(self, metric=None, start=None, stop=None,
                   aggregation='mean', reaggregation=None, granularity=None,
                   needed_overlap=100.0, fill=None,
                   refresh=False, resample=None):
        if pecan.request.method == 'GET':
            try:
                metric_ids = voluptuous.Schema(
                    self.MetricIDsSchema, required=True)(arg_to_list(metric))
            except voluptuous.Error as e:
                abort(400, "Invalid input: %s" % e)
        else:
            self._workaround_pecan_issue_88()
            metric_ids = deserialize_and_validate(self.MetricIDsSchema)

        metric_ids = [six.text_type(m) for m in metric_ids]
        # Check RBAC policy
        metrics = pecan.request.indexer.list_metrics(
            attribute_filter={"in": {"id": metric_ids}})
        missing_metric_ids = (set(metric_ids)
                              - set(six.text_type(m.id) for m in metrics))
        if missing_metric_ids:
            # Return one of the missing one in the error
            abort(404, six.text_type(storage.MetricDoesNotExist(
                missing_metric_ids.pop())))
        return self.get_cross_metric_measures_from_objs(
            metrics, start, stop, aggregation, reaggregation,
            granularity, needed_overlap, fill, refresh, resample)

    post_metric = get_metric

    def _workaround_pecan_issue_88(self):
        # FIXME(sileht): https://github.com/pecan/pecan/pull/88
        if pecan.request.path_info.startswith("/aggregation/resource"):
            pecan.abort(405)


class CapabilityController(rest.RestController):
    @staticmethod
    @pecan.expose('json')
    def get():
        return dict(aggregation_methods=set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS))


class StatusController(rest.RestController):
    @staticmethod
    @pecan.expose('json')
    def get(details=True):
        enforce("get status", {})
        try:
            members_req = pecan.request.coordinator.get_members(
                metricd.MetricProcessor.GROUP_ID)
        except tooz.NotImplemented:
            members_req = None
        try:
            report = pecan.request.incoming.measures_report(
                strtobool("details", details))
        except incoming.ReportGenerationError:
            abort(503, 'Unable to generate status. Please retry.')
        report_dict = {"storage": {"summary": report['summary']}}
        if 'details' in report:
            report_dict["storage"]["measures_to_process"] = report['details']
        report_dict['metricd'] = {}
        if members_req:
            members = members_req.get()
            caps = [
                pecan.request.coordinator.get_member_capabilities(
                    metricd.MetricProcessor.GROUP_ID, member)
                for member in members
            ]
            report_dict['metricd']['processors'] = members
            report_dict['metricd']['statistics'] = {
                member: cap.get()
                for member, cap in six.moves.zip(members, caps)
            }
        else:
            report_dict['metricd']['processors'] = None
            report_dict['metricd']['statistics'] = {}
        return report_dict


class MetricsBatchController(object):
    measures = MetricsMeasuresBatchController()


class ResourcesMetricsBatchController(object):
    measures = ResourcesMetricsMeasuresBatchController()


class ResourcesBatchController(object):
    metrics = ResourcesMetricsBatchController()


class BatchController(object):
    metrics = MetricsBatchController()
    resources = ResourcesBatchController()


# Retry with exponential backoff for up to 1 minute
@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=0.5, max=60),
    retry=tenacity.retry_if_exception_type(
        (indexer.NoSuchResource, indexer.ResourceAlreadyExists,
         indexer.ResourceTypeAlreadyExists,
         indexer.NamedMetricAlreadyExists)))
def get_or_create_resource_and_metrics(
        creator, rid, original_resource_id, metric_names,
        resource_attributes,
        resource_type, resource_type_attributes=None):
    try:
        r = pecan.request.indexer.get_resource(resource_type, rid,
                                               with_metrics=True)
    except indexer.NoSuchResourceType:
        if resource_type_attributes:
            enforce("create resource type", {
                'name': resource_type,
                'state': 'creating',
                'attributes': resource_type_attributes,
            })

            schema = pecan.request.indexer.get_resource_type_schema()
            rt = schema.resource_type_from_dict(
                resource_type, resource_type_attributes, 'creating')
            pecan.request.indexer.create_resource_type(rt)
            raise tenacity.TryAgain
        else:
            raise
    except indexer.UnexpectedResourceTypeState as e:
        # NOTE(sileht): Currently created by another thread
        if not e.state.endswith("_error"):
            raise tenacity.TryAgain

    if r:
        enforce("update resource", r)
        exists_metric_names = [m.name for m in r.metrics]
        metrics = MetricsSchema(dict(
            (m, {}) for m in metric_names
            if m not in exists_metric_names
        ))
        if metrics:
            return pecan.request.indexer.update_resource(
                resource_type, rid,
                metrics=metrics,
                append_metrics=True,
                create_revision=False
            ).metrics
        else:
            return r.metrics
    else:
        metrics = MetricsSchema(dict((m, {}) for m in metric_names))
        target = {
            "id": rid,
            "resource_type": resource_type,
            "creator": creator,
            "original_resource_id": original_resource_id,
            "metrics": metrics,
        }
        target.update(resource_attributes)
        enforce("create resource", target)

        kwargs = resource_attributes  # no copy used since not used after
        kwargs['metrics'] = metrics
        kwargs['original_resource_id'] = original_resource_id

        try:
            return pecan.request.indexer.create_resource(
                resource_type, rid, creator, **kwargs
            ).metrics
        except indexer.ResourceAlreadyExists as e:
            # NOTE(sileht): ensure the rid is not registered whitin another
            # resource type.
            r = pecan.request.indexer.get_resource('generic', rid)
            if r.type != resource_type:
                abort(409, e)
            raise


class PrometheusWriteController(rest.RestController):

    PROMETHEUS_RESOURCE_TYPE = {
        "instance": {"type": "string",
                     "min_length": 1,
                     "max_length": 512,
                     "required": True},
        "job": {"type": "string",
                "min_length": 1,
                "max_length": 512,
                "required": True}
    }

    @pecan.expose()
    def post(self):
        buf = snappy.uncompress(pecan.request.body)
        f = remote_pb2.WriteRequest()
        f.ParseFromString(buf)
        measures_by_rid = collections.defaultdict(dict)
        for ts in f.timeseries:
            attrs = dict((l.name, l.value) for l in ts.labels)
            original_rid = (attrs.get("job", "none"),
                            attrs.get("instance", "none"))
            name = attrs['__name__']
            if ts.samples:
                data = [{'timestamp': s.timestamp_ms / 1000.0,
                         'value': s.value} for s in ts.samples]
                measures_by_rid[original_rid][name] = validate(
                    MeasuresListSchema, data, detailed_exc=True)

        creator = pecan.request.auth_helper.get_current_user(pecan.request)

        measures_to_batch = {}
        for (job, instance), measures in measures_by_rid.items():
            original_rid = '%s@%s' % (job, instance)
            rid = ResourceUUID(original_rid, creator=creator)
            metric_names = list(measures.keys())
            timeout = pecan.request.conf.api.operation_timeout
            metrics = get_or_create_resource_and_metrics.retry_with(
                stop=tenacity.stop_after_delay(timeout))(
                    creator, rid, original_rid, metric_names,
                    dict(job=job, instance=instance),
                    "prometheus", self.PROMETHEUS_RESOURCE_TYPE)

            for metric in metrics:
                enforce("post measures", metric)

            measures_to_batch.update(
                dict((metric.id, measures[metric.name]) for metric in
                     metrics if metric.name in measures))

        pecan.request.incoming.add_measures_batch(measures_to_batch)
        pecan.response.status = 202


class PrometheusController(object):
    write = PrometheusWriteController()


class V1Controller(object):

    def __init__(self):
        # FIXME(sileht): split controllers to avoid lazy loading
        from gnocchi.rest.aggregates import api as agg_api
        from gnocchi.rest import influxdb

        self.sub_controllers = {
            "search": SearchController(),
            "archive_policy": ArchivePoliciesController(),
            "archive_policy_rule": ArchivePolicyRulesController(),
            "metric": MetricsController(),
            "batch": BatchController(),
            "resource": ResourcesByTypeController(),
            "resource_type": ResourceTypesController(),
            "aggregation": AggregationController(),
            "capabilities": CapabilityController(),
            "status": StatusController(),
            "aggregates": agg_api.AggregatesController(),
            "influxdb": influxdb.InfluxDBController(),
        }
        for name, ctrl in self.sub_controllers.items():
            setattr(self, name, ctrl)
        if PROMETHEUS_SUPPORTED:
            setattr(self, "prometheus", PrometheusController())

    @pecan.expose('json')
    def index(self):
        return {
            "version": "1.0",
            "links": [
                {"rel": "self",
                 "href": pecan.request.application_url}
            ] + [
                {"rel": name,
                 "href": pecan.request.application_url + "/" + name}
                for name in sorted(self.sub_controllers)
            ]
        }


class VersionsController(object):
    @staticmethod
    @pecan.expose('json')
    def index():
        return {
            "build": gnocchi.__version__,
            "versions": [
                {
                    "status": "CURRENT",
                    "links": [
                        {
                            "rel": "self",
                            "href": pecan.request.application_url + "/v1/"
                            }
                        ],
                    "id": "v1.0",
                    "updated": "2015-03-19"
                    }
                ]
            }
