# -*- encoding: utf-8 -*-
#
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
import fnmatch
import uuid

from oslo_log import log
from oslo_utils import strutils
from oslo_utils import timeutils
import pecan
from pecan import rest
import six
from six.moves.urllib import parse as urllib_parse
from stevedore import extension
import voluptuous
import webob.exc
import werkzeug.http

from gnocchi import aggregates
from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi import json
from gnocchi import storage
from gnocchi import utils

LOG = log.getLogger(__name__)


def arg_to_list(value):
    if isinstance(value, list):
        return value
    elif value:
        return [value]
    return []


def abort(status_code=None, detail='', headers=None, comment=None, **kw):
    """Like pecan.abort, but make sure detail is a string."""
    return pecan.abort(status_code, six.text_type(detail),
                       headers, comment, **kw)


def get_user_and_project():
    headers = pecan.request.headers
    # NOTE(jd) If user_id or project_id are UUID, try to convert them in the
    # proper dashed format. It's indeed possible that a middleware passes
    # theses UUID without the dash representation. It's valid, we can parse,
    # but the policy module won't see the equality in the string
    # representations.
    user_id = headers.get("X-User-Id")
    if user_id:
        try:
            user_id = six.text_type(uuid.UUID(user_id))
        except Exception:
            abort(400, "Malformed X-User-Id")

    project_id = headers.get("X-Project-Id")
    if project_id:
        try:
            project_id = six.text_type(uuid.UUID(project_id))
        except Exception:
            abort(400, "Malformed X-Project-Id")
    return (user_id, project_id)


# TODO(jd) Move this to oslo.utils as I stole it from Ceilometer
def recursive_keypairs(d, separator='.'):
    """Generator that produces sequence of keypairs for nested dictionaries."""
    for name, value in sorted(six.iteritems(d)):
        if isinstance(value, dict):
            for subname, subvalue in recursive_keypairs(value, separator):
                yield ('%s%s%s' % (name, separator, subname), subvalue)
        else:
            yield name, value


def enforce(rule, target):
    """Return the user and project the request should be limited to.

    :param rule: The rule name
    :param target: The target to enforce on.

    """
    headers = pecan.request.headers
    user_id, project_id = get_user_and_project()
    creds = {
        'roles': headers.get("X-Roles", "").split(","),
        'user_id': user_id,
        'project_id': project_id
    }

    if not isinstance(target, dict):
        target = target.__dict__

    # Flatten dict
    target = dict(recursive_keypairs(target))

    if not pecan.request.policy_enforcer.enforce(rule, target, creds):
        abort(403)


def set_resp_location_hdr(location):
    # NOTE(sileht): according the pep-3333 the headers must be
    # str in py2 and py3 even this is not the same thing in both
    # version
    # see: http://legacy.python.org/dev/peps/pep-3333/#unicode-issues
    if six.PY2 and isinstance(location, six.text_type):
        location = location.encode('utf-8')
    location = urllib_parse.quote(location)
    pecan.response.headers['Location'] = location


def deserialize(schema, required=True):
    mime_type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Content-Type'))
    if mime_type != "application/json":
        abort(415)
    try:
        params = json.loads(pecan.request.body.decode(
            options.get('charset', 'ascii')))
    except Exception as e:
        abort(400, "Unable to decode body: " + six.text_type(e))
    try:
        return voluptuous.Schema(schema, required=required)(params)
    except voluptuous.Error as e:
        abort(400, "Invalid input: %s" % e)


def Timestamp(v):
    if v is None:
        return v
    return utils.to_timestamp(v)


def convert_metric_list(metrics, created_by_user_id, created_by_project_id):
    # Replace an archive policy as value for an metric by a brand
    # a new metric
    new_metrics = {}
    for k, v in six.iteritems(metrics):
        if isinstance(v, uuid.UUID):
            new_metrics[k] = v
        else:
            new_metrics[k] = str(MetricsController.create_metric(
                created_by_user_id, created_by_project_id,
                v['archive_policy_name'])['id'])
    return new_metrics


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
    return utils.to_timespan(value).total_seconds()


def get_header_option(name, params):
    type, options = werkzeug.http.parse_options_header(
        pecan.request.headers.get('Accept'))
    try:
        return strutils.bool_from_string(
            options.get(name, params.pop(name, 'false')),
            strict=True)
    except ValueError as e:
        method = 'Accept' if name in options else 'query'
        abort(
            400,
            "Unable to parse %s value in %s: %s"
            % (name, method, six.text_type(e)))


def get_history(params):
    return get_header_option('history', params)


def get_details(params):
    return get_header_option('details', params)


def ValidAggMethod(value):
    value = six.text_type(value)
    if value in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS_VALUES:
        return value
    raise ValueError("Invalid aggregation method")


class ArchivePoliciesController(rest.RestController):
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
                default=set(conf.archive_policy.default_aggregation_methods)):
            [ValidAggMethod],
            voluptuous.Required("definition"):
            voluptuous.All([{
                "granularity": Timespan,
                "points": PositiveNotNullInt,
                "timespan": Timespan,
                }], voluptuous.Length(min=1)),
            })

        body = deserialize(ArchivePolicySchema)
        # Validate the data
        try:
            ap = archive_policy.ArchivePolicy.from_dict(body)
        except ValueError as e:
            abort(400, e)
        enforce("create archive policy", ap)
        try:
            ap = pecan.request.indexer.create_archive_policy(ap)
        except indexer.ArchivePolicyAlreadyExists as e:
            abort(409, e)

        location = "/v1/archive_policy/" + ap['name']
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_one(self, id):
        ap = pecan.request.indexer.get_archive_policy(id)
        if ap:
            enforce("get archive policy", ap)
            return ap
        abort(404)

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy", {})
        return pecan.request.indexer.list_archive_policies()

    @pecan.expose()
    def delete(self, name):
        try:
            pecan.request.indexer.delete_archive_policy(name)
        except indexer.NoSuchArchivePolicy as e:
            abort(404, e)
        except indexer.ArchivePolicyInUse as e:
            abort(400, e)


class ArchivePolicyRulesController(rest.RestController):
    _custom_actions = {
        'measures': ['GET', 'POST']
    }

    @pecan.expose('json')
    def post(self):
        enforce("create archive policy rule", {})
        ArchivePolicyRuleSchema = voluptuous.Schema({
            voluptuous.Required("name"): six.text_type,
            voluptuous.Required("metric_pattern"): six.text_type,
            voluptuous.Required("archive_policy_name"): six.text_type,
            })

        body = deserialize(ArchivePolicyRuleSchema)
        enforce("create archive policy rule", body)
        try:
            ap = pecan.request.indexer.create_archive_policy_rule(
                body['name'], body['metric_pattern'],
                body['archive_policy_name']
            )
        except indexer.ArchivePolicyRuleAlreadyExists as e:
            abort(409, e)

        location = "/v1/archive_policy_rule/" + ap['name']
        set_resp_location_hdr(location)
        pecan.response.status = 201
        return ap

    @pecan.expose('json')
    def get_one(self, name):
        ap = pecan.request.indexer.get_archive_policy_rule(name)
        if ap:
            enforce("get archive policy rule", ap)
            return ap
        abort(404)

    @pecan.expose('json')
    def get_all(self):
        enforce("list archive policy rule", {})
        return pecan.request.indexer.list_archive_policy_rules()

    @pecan.expose()
    def delete(self, name):
        try:
            pecan.request.indexer.delete_archive_policy_rule(name)
        except indexer.NoSuchArchivePolicyRule as e:
            abort(404, e)
        except indexer.ArchivePolicyRuleInUse as e:
            abort(400, e)


class AggregatedMetricController(rest.RestController):
    _custom_actions = {
        'measures': ['GET']
    }

    def __init__(self, metric_ids):
        self.metric_ids = metric_ids

    @pecan.expose('json')
    @pecan.expose('measures.j2')
    def get_measures(self, start=None, stop=None, aggregation='mean',
                     needed_overlap=100.0):
        return self.get_cross_metric_measures_from_ids(
            self.metric_ids, start, stop, aggregation, needed_overlap)

    @classmethod
    def get_cross_metric_measures_from_ids(cls, metric_ids, start=None,
                                           stop=None, aggregation='mean',
                                           needed_overlap=100.0):
        # Check RBAC policy
        metrics = pecan.request.indexer.get_metrics(metric_ids)
        missing_metric_ids = (set(metric_ids)
                              - set(six.text_type(m.id) for m in metrics))
        if missing_metric_ids:
            # Return one of the missing one in the error
            abort(404, storage.MetricDoesNotExist(
                missing_metric_ids.pop()))
        return cls.get_cross_metric_measures_from_objs(
            metrics, start, stop, aggregation, needed_overlap)

    @staticmethod
    def get_cross_metric_measures_from_objs(metrics, start=None, stop=None,
                                            aggregation='mean',
                                            needed_overlap=100.0):

        if (aggregation
           not in archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS):
            abort(
                400,
                'Invalid aggregation value %s, must be one of %s'
                % (aggregation,
                   archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS))

        for metric in metrics:
            enforce("get metric", metric)

        try:
            if len(metrics) == 1:
                # NOTE(sileht): don't do the aggregation if we only have one
                # metric
                # NOTE(jd): the archive policy is None as it's not really used
                # and it has a cost to request it from the indexer
                measures = pecan.request.storage.get_measures(
                    metrics[0], start, stop, aggregation)
            else:
                # NOTE(jd): the archive policy is None as it's not really used
                # and it has a cost to request it from the indexer
                measures = pecan.request.storage.get_cross_metric_measures(
                    metrics, start, stop, aggregation, needed_overlap)
            # Replace timestamp keys by their string versions
            return [(timeutils.isotime(timestamp, subsecond=True), offset, v)
                    for timestamp, offset, v in measures]
        except storage.MetricUnaggregatable:
            abort(400, "One of the metric to aggregated doesn't have "
                  "matching granularity")
        except storage.MetricDoesNotExist as e:
            abort(404, e)
        except storage.AggregationDoesNotExist as e:
            abort(404, e)


class MetricController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, metric):
        self.metric = metric
        mgr = extension.ExtensionManager(namespace='gnocchi.aggregates',
                                         invoke_on_load=True)
        self.custom_agg = dict((x.name, x.obj) for x in mgr)

    Measures = voluptuous.Schema([{
        voluptuous.Required("timestamp"):
        Timestamp,
        voluptuous.Required("value"): voluptuous.Any(float, int),
    }])

    def enforce_metric(self, rule):
        enforce(rule, json.to_primitive(self.metric))

    @pecan.expose('json')
    def get_all(self):
        self.enforce_metric("get metric")
        return self.metric

    @pecan.expose()
    def post_measures(self):
        self.enforce_metric("post measures")
        try:
            pecan.request.storage.add_measures(
                self.metric,
                (storage.Measure(
                    m['timestamp'],
                    m['value']) for m in deserialize(self.Measures)))
        except storage.MetricDoesNotExist as e:
            abort(404, e)
        except storage.NoDeloreanAvailable as e:
            abort(400,
                  "The measure for %s is too old considering the "
                  "archive policy used by this metric. "
                  "It can only go back to %s."
                  % (e.bad_timestamp, e.first_timestamp))

    @pecan.expose('json')
    @pecan.expose('measures.j2')
    def get_measures(self, start=None, stop=None, aggregation='mean', **param):
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
                start = Timestamp(start)
            except Exception:
                abort(400, "Invalid value for start")

        if stop is not None:
            try:
                stop = Timestamp(stop)
            except Exception:
                abort(400, "Invalid value for stop")

        try:
            if aggregation in self.custom_agg:
                measures = self.custom_agg[aggregation].compute(
                    pecan.request.storage, self.metric,
                    start, stop, **param)
            else:
                measures = pecan.request.storage.get_measures(
                    # NOTE(jd) We don't set the archive policy in the object
                    # here because it's not used; but we could do it if needed
                    # by requesting the metric details from the indexer
                    self.metric, start, stop, aggregation)
            # Replace timestamp keys by their string versions
            return [(timeutils.isotime(timestamp, subsecond=True), offset, v)
                    for timestamp, offset, v in measures]
        except storage.MetricDoesNotExist as e:
            abort(404, e)
        except storage.AggregationDoesNotExist as e:
            abort(404, e)
        except aggregates.CustomAggFailure as e:
            abort(400, e)

    @pecan.expose()
    def delete(self):
        self.enforce_metric("delete metric")
        try:
            pecan.request.storage.delete_metric(self.metric)
        except storage.MetricDoesNotExist as e:
            abort(404, e)
        pecan.request.indexer.delete_metric(self.metric.id)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


MetricSchemaDefinition = {
    "user_id": UUID,
    "project_id": UUID,
    "archive_policy_name": six.text_type,
    "name": six.text_type,
}


class MetricsController(rest.RestController):
    @staticmethod
    @pecan.expose()
    def _lookup(id, *remainder):
        try:
            metric_id = uuid.UUID(id)
        except ValueError:
            abort(404)
        metrics = pecan.request.indexer.get_metrics([metric_id])
        if not metrics:
            abort(404)
        return MetricController(metrics[0]), remainder

    Metric = voluptuous.Schema(MetricSchemaDefinition)

    @staticmethod
    def create_metric(created_by_user_id, created_by_project_id,
                      archive_policy_name=None, name=None,
                      user_id=None, project_id=None):
        enforce("create metric", {
            "created_by_user_id": created_by_user_id,
            "created_by_project_id": created_by_project_id,
            "user_id": user_id,
            "project_id": project_id,
            "archive_policy_name": archive_policy_name,
            "name": name
        })
        id = uuid.uuid4()
        policy = None
        if name is not None and archive_policy_name is None:
            rules = pecan.request.indexer.list_archive_policy_rules()
            for rule in rules:
                if fnmatch.fnmatch(name, rule['metric_pattern']):
                    policy = pecan.request.indexer.get_archive_policy(
                        rule['archive_policy_name'])
                    break
            else:
                abort(400, "No archive policy name specified and no archive"
                           " policy rule found matching the metric name %s"
                           % name)
        else:
            policy = pecan.request.indexer.get_archive_policy(
                archive_policy_name)
            if policy is None:
                abort(400, "Unknown archive policy %s" % archive_policy_name)
        m = pecan.request.indexer.create_metric(
            id,
            created_by_user_id, created_by_project_id,
            archive_policy_name=policy.name, name=name, details=True)
        pecan.request.storage.create_metric(storage.Metric(id, policy))
        return m

    @pecan.expose('json')
    def post(self):
        user, project = get_user_and_project()
        body = deserialize(self.Metric)
        metric_info = self.create_metric(user, project, **body)
        set_resp_location_hdr("/v1/metric/" + str(metric_info['id']))
        pecan.response.status = 201
        return metric_info

    @staticmethod
    @pecan.expose('json')
    def get_all(**kwargs):
        try:
            enforce("list all metric", {})
        except webob.exc.HTTPForbidden:
            enforce("list metric", {})
            user_id, project_id = get_user_and_project()
            provided_user_id = kwargs.get('user_id')
            provided_project_id = kwargs.get('project_id')
            if ((provided_user_id and user_id != provided_user_id)
               or (provided_project_id and project_id != provided_project_id)):
                abort(
                    403, "Insufficient privileges to filter by user/project")
        else:
            user_id = kwargs.get('user_id')
            project_id = kwargs.get('project_id')
        return pecan.request.indexer.list_metrics(
            user_id, project_id)


Metrics = voluptuous.Schema({
    six.text_type: voluptuous.Any(UUID,
                                  MetricsController.Metric),
})


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

        abort(404)

    @pecan.expose()
    def post(self):
        resource = pecan.request.indexer.get_resource(
            self.resource_type, self.resource_id)
        enforce("update resource", resource)
        user, project = get_user_and_project()
        metrics = convert_metric_list(deserialize(Metrics), user, project)
        try:
            pecan.request.indexer.update_resource(
                self.resource_type, self.resource_id, metrics=metrics,
                append_metrics=True)
        except (indexer.NoSuchMetric, ValueError) as e:
            abort(400, e)
        except indexer.NamedMetricAlreadyExists as e:
            abort(409, e)
        except indexer.NoSuchResource as e:
            abort(404, e)


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


def ResourceSchema(schema):
    base_schema = {
        "id": UUID,
        voluptuous.Optional('started_at'): Timestamp,
        voluptuous.Optional('ended_at'): Timestamp,
        voluptuous.Optional('user_id'): voluptuous.Any(None, UUID),
        voluptuous.Optional('project_id'): voluptuous.Any(None, UUID),
        voluptuous.Optional('metrics'): Metrics,
    }
    base_schema.update(schema)
    return base_schema


class GenericResourceController(rest.RestController):
    _resource_type = 'generic'

    Resource = ResourceSchema({})

    def __init__(self, id):
        try:
            self.id = uuid.UUID(id)
        except ValueError:
            abort(404)
        self.metric = NamedMetricController(id, self._resource_type)

    @pecan.expose('json')
    @pecan.expose('resources.j2')
    def get(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id, with_metrics=True)
        if resource:
            enforce("get resource", resource)
            etag_precondition_check(resource)
            etag_set_headers(resource)
            return resource
        abort(404)

    @pecan.expose('json')
    @pecan.expose('resources.j2')
    def patch(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if not resource:
            abort(404)
        enforce("update resource", resource)
        etag_precondition_check(resource)

        body = deserialize(self.Resource, required=False)
        if len(body) == 0:
            etag_set_headers(resource)
            return resource

        try:
            if 'metrics' in body:
                user, project = get_user_and_project()
                body['metrics'] = convert_metric_list(
                    body['metrics'], user, project)
            resource = pecan.request.indexer.update_resource(
                self._resource_type,
                self.id, **body)
        except (indexer.NoSuchMetric, ValueError) as e:
            abort(400, e)
        except indexer.NoSuchResource as e:
            abort(404, e)
        etag_set_headers(resource)
        return resource

    @staticmethod
    def _delete_metrics(metrics):
        for metric in metrics:
            enforce("delete metric", metric)
        for metric in metrics:
            try:
                pecan.request.storage.delete_metric(metric)
            except Exception:
                LOG.error(
                    "Unable to delete metric `%s' from storage, "
                    "you will need to delete it manually" % metric.id,
                    exc_info=True)

    @pecan.expose()
    def delete(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if not resource:
            abort(404, indexer.NoSuchResource(self.id))
        enforce("delete resource", resource)
        etag_precondition_check(resource)
        try:
            pecan.request.indexer.delete_resource(
                self.id,
                delete_metrics=self._delete_metrics)
        except indexer.NoSuchResource as e:
            abort(404, e)


class SwiftAccountResourceController(GenericResourceController):
    _resource_type = 'swift_account'


class InstanceResourceController(GenericResourceController):
    _resource_type = 'instance'

    Resource = ResourceSchema({
        "flavor_id": int,
        "image_ref": six.text_type,
        "host": six.text_type,
        "display_name": six.text_type,
        voluptuous.Optional("server_group"): six.text_type,
    })


class VolumeResourceController(GenericResourceController):
    _resource_type = 'volume'

    Resource = ResourceSchema({
        "display_name": six.text_type,
    })


class CephAccountResourceController(GenericResourceController):
    _resource_type = 'ceph_account'


class NetworkResourceController(GenericResourceController):
    _resource_type = 'network'


class IdentityResourceController(GenericResourceController):
    _resource_type = 'identity'


class IPMIResourceController(GenericResourceController):
    _resource_type = 'ipmi'


class StackResourceController(GenericResourceController):
    _resource_type = 'stack'


class ImageResourceController(GenericResourceController):
    _resource_type = 'image'

    Resource = ResourceSchema({
        "name": six.text_type,
        "container_format": six.text_type,
        "disk_format": six.text_type,
    })


class GenericResourcesController(rest.RestController):
    _resource_type = 'generic'
    _resource_rest_class = GenericResourceController

    Resource = GenericResourceController.Resource

    @pecan.expose()
    def _lookup(self, id, *remainder):
        return self._resource_rest_class(id), remainder

    @pecan.expose('json')
    def post(self):
        body = deserialize(self.Resource)
        target = {
            "resource_type": self._resource_type,
        }
        target.update(body)
        enforce("create resource", target)
        user, project = get_user_and_project()
        body['metrics'] = convert_metric_list(
            body.get('metrics', {}), user, project)
        rid = body['id']
        del body['id']
        try:
            resource = pecan.request.indexer.create_resource(
                self._resource_type, rid, user, project,
                **body)
        except (ValueError, indexer.NoSuchMetric) as e:
            abort(400, e)
        except indexer.ResourceAlreadyExists as e:
            abort(409, e)
        set_resp_location_hdr("/v1/resource/"
                              + self._resource_type + "/"
                              + six.text_type(resource['id']))
        etag_set_headers(resource)
        pecan.response.status = 201
        return resource

    @pecan.expose('json')
    def get_all(self, **kwargs):
        details = get_details(kwargs)
        history = get_history(kwargs)

        try:
            enforce("list all resource", {
                "resource_type": self._resource_type,
            })
        except webob.exc.HTTPForbidden:
            enforce("list resource", {
                "resource_type": self._resource_type,
            })
            user, project = get_user_and_project()
            attr_filter = {"and": [{"=": {"created_by_user_id": user}},
                                   {"=": {"created_by_project_id": project}}]}
        else:
            attr_filter = None

        try:
            return pecan.request.indexer.list_resources(
                self._resource_type,
                attribute_filter=attr_filter,
                details=details,
                history=history)
        except indexer.IndexerException as e:
            abort(400, e)


class SwiftAccountsResourcesController(GenericResourcesController):
    _resource_type = 'swift_account'
    _resource_rest_class = SwiftAccountResourceController


class InstancesResourcesController(GenericResourcesController):
    _resource_type = 'instance'
    _resource_rest_class = InstanceResourceController

    Resource = InstanceResourceController.Resource


class VolumesResourcesController(GenericResourcesController):
    _resource_type = 'volume'
    _resource_rest_class = VolumeResourceController

    Resource = VolumeResourceController.Resource


class CephAccountsResourcesController(GenericResourcesController):
    _resource_type = 'ceph_account'
    _resource_rest_class = CephAccountResourceController


class NetworkResourcesController(GenericResourcesController):
    _resource_type = 'network'
    _resource_rest_class = NetworkResourceController


class IdentityResourcesController(GenericResourcesController):
    _resource_type = 'identity'
    _resource_rest_class = IdentityResourceController


class IPMIResourcesController(GenericResourcesController):
    _resource_type = 'ipmi'
    _resource_rest_class = IPMIResourceController


class StackResourcesController(GenericResourcesController):
    _resource_type = 'stack'
    _resource_rest_class = StackResourceController


class ImageResourcesController(GenericResourcesController):
    _resource_type = 'image'
    _resource_rest_class = ImageResourceController

    Resource = ImageResourceController.Resource


class ResourcesController(rest.RestController):
    resources_class_by_type = dict(
        (ext.name, ext.plugin)
        for ext in extension.ExtensionManager(
            'gnocchi.controller.resources').extensions)

    @pecan.expose('json')
    def get_all(self):
        return dict(
            (type_name,
             pecan.request.application_url + '/v1/resource/' + type_name)
            for type_name in self.resources_class_by_type.keys())


for resource_type, resource_class in (
        ResourcesController.resources_class_by_type.items()):
    setattr(ResourcesController, resource_type, resource_class())


def _ResourceSearchSchema(v):
    """Helper method to indirect the recursivity of the search schema"""
    return SearchResourceTypeController.ResourceSearchSchema(v)


class SearchResourceTypeController(rest.RestController):
    def __init__(self, resource_type):
        self._resource_type = resource_type

    ResourceSearchSchema = voluptuous.Schema(
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
                    u"in",
                    u"like",
                ): voluptuous.All(voluptuous.Length(min=1, max=1), dict),
                voluptuous.Any(
                    u"and", u"∨",
                    u"or", u"∧",
                    u"not",
                ): [_ResourceSearchSchema],
            }
        )
    )

    @pecan.expose('json')
    def post(self, **kwargs):
        if pecan.request.body:
            attr_filter = deserialize(self.ResourceSearchSchema)
        else:
            attr_filter = None

        details = get_details(kwargs)
        history = get_history(kwargs)

        try:
            enforce("search all resource", {
                "resource_type": self._resource_type,
            })
        except webob.exc.HTTPForbidden:
            enforce("search resource", {
                "resource_type": self._resource_type,
            })
            user, project = get_user_and_project()
            if attr_filter:
                attr_filter = {"and": [
                    {"=": {"created_by_user_id": user}},
                    {"=": {"created_by_project_id": project}},
                    attr_filter]}
            else:
                attr_filter = {"and": [
                    {"=": {"created_by_user_id": user}},
                    {"=": {"created_by_project_id": project}},
                ]}

        try:
            return pecan.request.indexer.list_resources(
                self._resource_type,
                attribute_filter=attr_filter,
                details=details,
                history=history)
        except indexer.IndexerException as e:
            abort(400, e)


class SearchResourceController(rest.RestController):
    @pecan.expose()
    def _lookup(self, resource_type, *remainder):
        # TODO(jd) Check that resource_type is valid
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
    def post(self, metric_id, start=None, stop=None, aggregation='mean'):
        metrics = pecan.request.indexer.get_metrics(arg_to_list(metric_id))

        for metric in metrics:
            enforce("search metric", metric)

        if not pecan.request.body:
            abort(400, "No query specified in body")

        query = deserialize(self.MetricSearchSchema)

        try:
            return {
                str(metric.id): values
                for metric, values in six.iteritems(
                    pecan.request.storage.search_value(
                        # NOTE(jd) Don't pass the archive policy as no
                        # driver needs it for now
                        [storage.Metric(str(metric['id']), None)
                         for metric in metrics],
                        query, start, stop, aggregation)
                )
            }
        except storage.InvalidQuery as e:
            abort(400, e)


class SearchController(rest.RestController):
    resource = SearchResourceController()
    metric = SearchMetricController()


class AggregationResource(rest.RestController):
    def __init__(self, resource_type, metric_name):
        self.resource_type = resource_type
        self.metric_name = metric_name

    @pecan.expose('json')
    @pecan.expose('measures.j2')
    def post(self, start=None, stop=None, aggregation='mean',
             needed_overlap=100.0):
        resources = SearchResourceTypeController(self.resource_type).post()
        metrics = []
        for r in resources:
            m = r.get_metric(self.metric_name)
            if m:
                metrics.append(m)
        return AggregatedMetricController.get_cross_metric_measures_from_objs(
            metrics, start, stop, aggregation, needed_overlap)


class Aggregation(rest.RestController):
    _custom_actions = {
        'metric': ['GET'],
    }

    @pecan.expose()
    def _lookup(self, object_type, subtype, key, metric_name, *remainder):
        if object_type == 'resource' and key == 'metric':
            return AggregationResource(subtype, metric_name), remainder
        return super(Aggregation, self)._lookup(object_type, subtype, key,
                                                metric_name, *remainder)

    @pecan.expose('json')
    @pecan.expose('measures.j2')
    def get_metric(self, metric=None, start=None,
                   stop=None, aggregation='mean',
                   needed_overlap=100.0):
        return AggregatedMetricController.get_cross_metric_measures_from_ids(
            arg_to_list(metric), start, stop, aggregation, needed_overlap)


class V1Controller(rest.RestController):
    search = SearchController()

    archive_policy = ArchivePoliciesController()
    archive_policy_rule = ArchivePolicyRulesController()
    metric = MetricsController()
    resource = ResourcesController()
    aggregation = Aggregation()

    _custom_actions = {
        'capabilities': ['GET'],
    }

    @staticmethod
    @pecan.expose('json')
    def get_capabilities():
        aggregation_methods = set(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS)
        aggregation_methods.update(
            ext.name for ext in extension.ExtensionManager(
                namespace='gnocchi.aggregates'))
        return dict(aggregation_methods=aggregation_methods)


class RootController(object):
    v1 = V1Controller()

    @staticmethod
    @pecan.expose('json')
    def index():
        return {
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
