# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
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
import functools
import itertools
import uuid
import warnings

import jsonpatch
import pbr.version
import pecan
from pecan import rest
import pyparsing
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import extension
import voluptuous
import webob.exc
import werkzeug.http

from gnocchi import archive_policy
from gnocchi import deprecated_aggregates
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import json
from gnocchi import resource_type
from gnocchi.rest.aggregates import exceptions
from gnocchi.rest.aggregates import processor
from gnocchi import storage
from gnocchi import utils


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


def validate(schema, data, required=True):
    try:
        return voluptuous.Schema(schema, required=required)(data)
    except voluptuous.Error as e:
        abort(400, "Invalid input: %s" % e)


def deserialize_and_validate(schema, required=True,
                             expected_content_types=None):
    return validate(schema,
                    deserialize(expected_content_types=expected_content_types),
                    required)


def PositiveOrNullInt(value):
    value = int(value)
    if value < 0:
        raise ValueError("Value must be positive")
    return value


def PositiveNotNullInt(value):
    value = int(value)
    if value <= 0:
        raise ValueError("Value must be positive and not null")
    return value


def Timespan(value):
    try:
        return utils.to_timespan(value)
    except ValueError as e:
        raise voluptuous.Invalid(e)


def get_header_option(name, params):
    type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Accept'))
    return strtobool('Accept header' if name in options else name,
                     options.get(name, params.pop(name, 'false')))


def get_history(params):
    return get_header_option('history', params)


def get_details(params):
    return get_header_option('details', params)


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
    max_limit = pecan.request.conf.api.max_limit
    limit = params.pop('limit', max_limit)
    marker = params.pop('marker', None)
    sorts = params.pop('sort', default)
    if not isinstance(sorts, list):
        sorts = [sorts]

    try:
        limit = PositiveNotNullInt(limit)
    except ValueError:
        abort(400, "Invalid 'limit' value: %s" % params.get('limit'))

    limit = min(limit, max_limit)

    return {'limit': limit,
            'marker': marker,
            'sorts': sorts}


def ValidAggMethod(value):
    value = six.text_type(value)
    if value in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS_VALUES:
        return value
    raise ValueError("Invalid aggregation method")


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
            voluptuous.Required("definition"):
                voluptuous.All([{
                    "granularity": Timespan,
                    "points": PositiveNotNullInt,
                    "timespan": Timespan}], voluptuous.Length(min=1)),
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
        # NOTE(jd): Initialize this one at run-time because we rely on conf
        conf = pecan.request.conf
        enforce("create archive policy", {})
        ArchivePolicySchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("back_window", default=0): PositiveOrNullInt,
            voluptuous.Required(
                "aggregation_methods",
                default=list(conf.archive_policy.default_aggregation_methods)):
            [ValidAggMethod],
            voluptuous.Required("definition"):
            voluptuous.All([{
                "granularity": Timespan,
                "points": PositiveNotNullInt,
                "timespan": Timespan,
                }], voluptuous.Length(min=1)),
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
        times = utils.to_timestamps((m['timestamp'] for m in measures))
    except TypeError:
        abort(400, "Invalid format for measures")
    except ValueError as e:
        abort(400, "Invalid input for timestamp: %s" % e)

    try:
        values = [float(i['value']) for i in measures]
    except Exception:
        abort(400, "Invalid input for a value")

    return (incoming.Measure(t, v) for t, v in six.moves.zip(
        times.tolist(), values))


class MetricController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, metric):
        self.metric = metric
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in mgr)

    def enforce_metric(self, rule):
        enforce(rule, json.to_primitive(self.metric))

    @pecan.expose('json')
    def get_all(self):
        self.enforce_metric("get metric")
        return self.metric

    @pecan.expose()
    def post_measures(self):
        self.enforce_metric("post measures")
        params = deserialize()
        if not isinstance(params, list):
            abort(400, "Invalid input for measures")
        if params:
            pecan.request.incoming.add_measures(
                self.metric, MeasuresListSchema(params))
        pecan.response.status = 202

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean',
                     granularity=None, resample=None, refresh=False,
                     **param):
        self.enforce_metric("get measures")
        if not (aggregation
                in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS
                or aggregation in self.custom_agg):
            msg = '''Invalid aggregation value %(agg)s, must be one of %(std)s
                     or %(custom)s'''
            abort(400, msg % dict(
                agg=aggregation,
                std=archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS,
                custom=str(self.custom_agg.keys())))

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

        if granularity is not None:
            try:
                granularity = utils.to_timespan(granularity)
            except ValueError:
                abort(400, {"cause": "Attribute value error",
                            "detail": "granularity",
                            "reason": "Invalid granularity"})

        if resample:
            if not granularity:
                abort(400, 'A granularity must be specified to resample')
            try:
                resample = utils.to_timespan(resample)
            except ValueError as e:
                abort(400, six.text_type(e))

        if (strtobool("refresh", refresh) and
                pecan.request.incoming.has_unprocessed(self.metric)):
            try:
                pecan.request.storage.refresh_metric(
                    pecan.request.indexer, pecan.request.incoming, self.metric,
                    pecan.request.conf.api.refresh_timeout)
            except storage.SackLockTimeoutError as e:
                abort(503, six.text_type(e))
        try:
            if aggregation in self.custom_agg:
                warnings.warn("moving_average aggregation is deprecated.",
                              category=DeprecationWarning)
                return self.custom_agg[aggregation].compute(
                    pecan.request.storage, self.metric,
                    start, stop, **param)
            return pecan.request.storage.get_measures(
                self.metric, start, stop, aggregation,
                granularity, resample)
        except (storage.MetricDoesNotExist,
                storage.GranularityDoesNotExist,
                storage.AggregationDoesNotExist) as e:
            abort(404, six.text_type(e))
        except deprecated_aggregates.CustomAggFailure as e:
            abort(400, six.text_type(e))

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
            id=metric_id, details=True)
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
        "limit": six.text_type,
        "name": six.text_type,
        "id": six.text_type,
        "unit": six.text_type,
        "archive_policy_name": six.text_type,
        "status": voluptuous.Any("active", "delete"),
        "sort": voluptuous.Any([six.text_type], six.text_type),
        "marker": six.text_type,
    })

    @classmethod
    @pecan.expose('json')
    def get_all(cls, **kwargs):
        kwargs = cls.MetricListSchema(kwargs)

        # Compat with old user/project API
        provided_user_id = kwargs.pop('user_id', None)
        provided_project_id = kwargs.pop('project_id', None)
        if provided_user_id is None and provided_project_id is None:
            provided_creator = kwargs.pop('creator', None)
        else:
            provided_creator = (
                (provided_user_id or "")
                + ":"
                + (provided_project_id or "")
            )
        try:
            enforce("list all metric", {})
        except webob.exc.HTTPForbidden:
            enforce("list metric", {})
            creator = pecan.request.auth_helper.get_current_user(
                pecan.request)
            if provided_creator and creator != provided_creator:
                abort(403, "Insufficient privileges to filter by user/project")
            provided_creator = creator

        pagination_opts = get_pagination_options(kwargs,
                                                 METRIC_DEFAULT_PAGINATION)
        attr_filter = {}
        if provided_creator is not None:
            attr_filter['creator'] = provided_creator
        attr_filter.update(pagination_opts)
        attr_filter.update(kwargs)
        try:
            metrics = pecan.request.indexer.list_metrics(**attr_filter)
            if metrics and len(metrics) >= pagination_opts['limit']:
                set_resp_link_hdr(str(metrics[-1].id), kwargs, pagination_opts)
            return metrics
        except indexer.IndexerException as e:
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
        m = pecan.request.indexer.list_metrics(details=True,
                                               name=name,
                                               resource_id=self.resource_id)
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
        return pecan.request.indexer.list_metrics(resource_id=self.resource_id)


class ResourceHistoryController(rest.RestController):
    def __init__(self, resource_id, resource_type):
        self.resource_id = resource_id
        self.resource_type = resource_type

    @pecan.expose('json')
    def get(self, **kwargs):
        initial_kwargs = kwargs.copy()
        details = get_details(kwargs)
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
                set_resp_link_hdr(marker, initial_kwargs, pagination_opts)
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
        initial_kwargs = kwargs.copy()
        details = get_details(kwargs)
        history = get_history(kwargs)
        pagination_opts = get_pagination_options(
            kwargs, RESOURCE_DEFAULT_PAGINATION)
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
                set_resp_link_hdr(marker, initial_kwargs, pagination_opts)
            return resources
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
            raise abort(400, "Invalid filter: %s" % six.text_type(e))
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
        initial_kwargs = kwargs.copy()
        if pecan.request.body:
            attr_filter = deserialize_and_validate(ResourceSearchSchema)
        elif kwargs.get("filter"):
            attr_filter = QueryStringSearchAttrFilter.parse(kwargs["filter"])
        else:
            attr_filter = None

        details = get_details(kwargs)
        history = get_history(kwargs)
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
            set_resp_link_hdr(marker, initial_kwargs, pagination_opts)
        return resources

    @pecan.expose('json')
    def post(self, **kwargs):
        try:
            return self._search(**kwargs)
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

    @pecan.expose('json')
    def post(self, metric_id, start=None, stop=None, aggregation='mean',
             granularity=None):
        granularity = [utils.to_timespan(g)
                       for g in arg_to_list(granularity or [])]
        metrics = pecan.request.indexer.list_metrics(
            ids=arg_to_list(metric_id))

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
            return {
                str(metric.id): values
                for metric, values in six.iteritems(
                    pecan.request.storage.search_value(
                        metrics, query, start, stop, aggregation,
                        granularity
                    )
                )
            }
        except storage.InvalidQuery as e:
            abort(400, six.text_type(e))
        except storage.GranularityDoesNotExist as e:
            abort(400, six.text_type(e))


class ResourcesMetricsMeasuresBatchController(rest.RestController):
    @pecan.expose('json')
    def post(self, create_metrics=False):
        creator = pecan.request.auth_helper.get_current_user(
            pecan.request)
        MeasuresBatchSchema = voluptuous.Schema(
            {functools.partial(ResourceID, creator=creator):
             {six.text_type: MeasuresListSchema}}
        )

        body = deserialize_and_validate(MeasuresBatchSchema)

        known_metrics = []
        unknown_metrics = []
        unknown_resources = []
        body_by_rid = {}
        for original_resource_id, resource_id in body:
            body_by_rid[resource_id] = body[(original_resource_id,
                                             resource_id)]
            names = body[(original_resource_id, resource_id)].keys()
            metrics = pecan.request.indexer.list_metrics(
                names=names, resource_id=resource_id)

            known_names = [m.name for m in metrics]
            if strtobool("create_metrics", create_metrics):
                already_exists_names = []
                for name in names:
                    if name not in known_names:
                        metric = MetricsController.MetricSchema({
                            "name": name
                        })
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
                            names=already_exists_names,
                            resource_id=resource_id)
                    )

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
            dict((metric,
                 body_by_rid[metric.resource_id][metric.name])
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

    @pecan.expose()
    def post(self):
        body = deserialize_and_validate(self.MeasuresBatchSchema)
        metrics = pecan.request.indexer.list_metrics(ids=body.keys())

        if len(metrics) != len(body):
            missing_metrics = sorted(set(body) - set(m.id for m in metrics))
            abort(400, "Unknown metrics: %s" % ", ".join(
                six.moves.map(str, missing_metrics)))

        for metric in metrics:
            enforce("post measures", metric)

        pecan.request.incoming.add_measures_batch(
            dict((metric, body[metric.id]) for metric in
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
             groupby=None, fill=None, refresh=False, resample=None):
        # First, set groupby in the right format: a sorted list of unique
        # strings.
        groupby = sorted(set(arg_to_list(groupby)))

        # NOTE(jd) Sort by groupby so we are sure we do not return multiple
        # groups when using itertools.groupby later.
        try:
            resources = SearchResourceTypeController(
                self.resource_type)._search(sort=groupby)
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


# FIXME(sileht): should be in aggregates.api but we need to split all
# controllers to do this
def validate_qs(start, stop, granularity, needed_overlap, fill):
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
            granularity = utils.to_timespan(granularity)
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

        if (aggregation
           not in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS):
            abort(
                400,
                'Invalid aggregation value %s, must be one of %s'
                % (aggregation,
                   archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS))

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
                resample = utils.to_timespan(resample)
            except ValueError as e:
                abort(400, six.text_type(e))

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
                    if pecan.request.incoming.has_unprocessed(m)]
                for m in metrics_to_update:
                    try:
                        pecan.request.storage.refresh_metric(
                            pecan.request.indexer, pecan.request.incoming, m,
                            pecan.request.conf.api.refresh_timeout)
                    except storage.SackLockTimeoutError as e:
                        abort(503, six.text_type(e))
            if number_of_metrics == 1:
                # NOTE(sileht): don't do the aggregation if we only have one
                # metric
                return pecan.request.storage.get_measures(
                    metrics[0], start, stop, aggregation,
                    granularity, resample)
            return processor.get_measures(
                pecan.request.storage,
                [processor.MetricReference(m, aggregation) for m in metrics],
                operations, start, stop,
                granularity, needed_overlap, fill)["aggregated"]
        except exceptions.UnAggregableTimeseries as e:
            abort(400, e)
        except (storage.MetricDoesNotExist,
                storage.GranularityDoesNotExist,
                storage.AggregationDoesNotExist) as e:
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
        metrics = pecan.request.indexer.list_metrics(ids=metric_ids)
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
        aggregation_methods = set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)
        return dict(aggregation_methods=aggregation_methods,
                    dynamic_aggregation_methods=[
                        ext.name for ext in extension.ExtensionManager(
                            # NOTE(sileht): Known as deprecated_aggregates
                            # but we can't change the namespace
                            namespace='gnocchi.aggregates')
                    ])


class StatusController(rest.RestController):
    @staticmethod
    @pecan.expose('json')
    def get(details=True):
        enforce("get status", {})
        try:
            report = pecan.request.incoming.measures_report(
                strtobool("details", details))
        except incoming.ReportGenerationError:
            abort(503, 'Unable to generate status. Please retry.')
        report_dict = {"storage": {"summary": report['summary']}}
        if 'details' in report:
            report_dict["storage"]["measures_to_process"] = report['details']
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


class V1Controller(object):

    def __init__(self):
        # FIXME(sileht): split controllers to avoid lazy loading
        from gnocchi.rest.aggregates import api as agg_api

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
        }
        for name, ctrl in self.sub_controllers.items():
            setattr(self, name, ctrl)

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
            "build": pbr.version.VersionInfo('gnocchi').version_string(),
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
