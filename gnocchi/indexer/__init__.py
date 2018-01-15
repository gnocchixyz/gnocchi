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
import hashlib
import os

import iso8601
from oslo_config import cfg
import six
from six.moves.urllib import parse
from stevedore import driver

from gnocchi import exceptions
from gnocchi import utils

OPTS = [
    cfg.StrOpt('url',
               secret=True,
               required=True,
               default=os.getenv("GNOCCHI_INDEXER_URL"),
               help='Indexer driver to use'),
]


_marker = object()


class Resource(object):
    def get_metric(self, metric_name):
        for m in self.metrics:
            if m.name == metric_name:
                return m

    def __eq__(self, other):
        return (self.id == other.id
                and self.type == other.type
                and self.revision == other.revision
                and self.revision_start == other.revision_start
                and self.revision_end == other.revision_end
                and self.creator == other.creator
                and self.user_id == other.user_id
                and self.project_id == other.project_id
                and self.started_at == other.started_at
                and self.ended_at == other.ended_at)

    @property
    def etag(self):
        etag = hashlib.sha1()
        etag.update(six.text_type(self.id).encode('utf-8'))
        etag.update(six.text_type(
            self.revision_start.isoformat()).encode('utf-8'))
        return etag.hexdigest()

    @property
    def lastmodified(self):
        # less precise revision start for Last-Modified http header
        return self.revision_start.replace(microsecond=0,
                                           tzinfo=iso8601.iso8601.UTC)

    __hash__ = object.__hash__


class Metric(object):
    def __init__(self, id, archive_policy, creator=None,
                 name=None, resource_id=None):
        self.id = id
        self.archive_policy = archive_policy
        self.creator = creator
        self.name = name
        self.resource_id = resource_id

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.id)

    def __str__(self):
        return str(self.id)

    def __eq__(self, other):
        return (isinstance(other, Metric)
                and self.id == other.id
                and self.archive_policy == other.archive_policy
                and self.creator == other.creator
                and self.name == other.name
                and self.resource_id == other.resource_id)

    __hash__ = object.__hash__


@utils.retry_on_exception_and_log("Unable to initialize indexer driver")
def get_driver(conf):
    """Return the configured driver."""
    split = parse.urlsplit(conf.indexer.url)
    d = driver.DriverManager('gnocchi.indexer',
                             split.scheme).driver
    return d(conf)


class IndexerException(Exception):
    """Base class for all exceptions raised by an indexer."""


class NoSuchResourceType(IndexerException):
    """Error raised when the resource type is unknown."""
    def __init__(self, type):
        super(NoSuchResourceType, self).__init__(
            "Resource type %s does not exist" % type)
        self.type = type

    def jsonify(self):
        return {
            "cause": "Resource type does not exist",
            "detail": self.type,
        }


class NoSuchMetric(IndexerException):
    """Error raised when a metric does not exist."""
    def __init__(self, metric):
        super(NoSuchMetric, self).__init__("Metric %s does not exist" %
                                           metric)
        self.metric = metric


class NoSuchResource(IndexerException):
    """Error raised when a resource does not exist."""
    def __init__(self, resource):
        super(NoSuchResource, self).__init__("Resource %s does not exist" %
                                             resource)
        self.resource = resource


class NoSuchArchivePolicy(IndexerException):
    """Error raised when an archive policy does not exist."""
    def __init__(self, archive_policy):
        super(NoSuchArchivePolicy, self).__init__(
            "Archive policy %s does not exist" % archive_policy)
        self.archive_policy = archive_policy

    def jsonify(self):
        return {
            "cause": "Archive policy does not exist",
            "detail": self.archive_policy,
        }


class UnsupportedArchivePolicyChange(IndexerException):
    """Error raised when modifying archive policy if not supported."""
    def __init__(self, archive_policy, message):
        super(UnsupportedArchivePolicyChange, self).__init__(
            "Archive policy %s does not support change: %s" %
            (archive_policy, message))
        self.archive_policy = archive_policy
        self.message = message


class ArchivePolicyInUse(IndexerException):
    """Error raised when an archive policy is still being used."""
    def __init__(self, archive_policy):
        super(ArchivePolicyInUse, self).__init__(
            "Archive policy %s is still in use" % archive_policy)
        self.archive_policy = archive_policy


class ResourceTypeInUse(IndexerException):
    """Error raised when an resource type is still being used."""
    def __init__(self, resource_type):
        super(ResourceTypeInUse, self).__init__(
            "Resource type %s is still in use" % resource_type)
        self.resource_type = resource_type


class UnexpectedResourceTypeState(IndexerException):
    """Error raised when an resource type state is not expected."""
    def __init__(self, resource_type, expected_state, state):
        super(UnexpectedResourceTypeState, self).__init__(
            "Resource type %s state is %s (expected: %s)" % (
                resource_type, state, expected_state))
        self.resource_type = resource_type
        self.expected_state = expected_state
        self.state = state


class NoSuchArchivePolicyRule(IndexerException):
    """Error raised when an archive policy rule does not exist."""
    def __init__(self, archive_policy_rule):
        super(NoSuchArchivePolicyRule, self).__init__(
            "Archive policy rule %s does not exist" %
            archive_policy_rule)
        self.archive_policy_rule = archive_policy_rule


class NoArchivePolicyRuleMatch(IndexerException):
    """Error raised when no archive policy rule found for metric."""
    def __init__(self, metric_name):
        super(NoArchivePolicyRuleMatch, self).__init__(
            "No Archive policy rule found for metric %s" %
            metric_name)
        self.metric_name = metric_name


class UnsupportedArchivePolicyRuleChange(IndexerException):
    """Error raised when modifying archive policy rule if not supported."""
    def __init__(self, archive_policy_rule, message):
        super(UnsupportedArchivePolicyRuleChange, self).__init__(
            "Archive policy rule %s does not support change: %s" %
            (archive_policy_rule, message))
        self.archive_policy_rule = archive_policy_rule
        self.message = message


class NamedMetricAlreadyExists(IndexerException):
    """Error raised when a named metric already exists."""
    def __init__(self, metric_name):
        super(NamedMetricAlreadyExists, self).__init__(
            "Named metric %s already exists" % metric_name)
        self.metric_name = metric_name

    def jsonify(self):
        return {"cause": "Named metric already exists",
                "detail": self.metric_name}


class ResourceAlreadyExists(IndexerException):
    """Error raised when a resource already exists."""
    def __init__(self, resource):
        super(ResourceAlreadyExists, self).__init__(
            "Resource %s already exists" % resource)
        self.resource = resource

    def jsonify(self):
        return {"cause": "Resource already exists",
                "detail": self.resource}


class ResourceTypeAlreadyExists(IndexerException):
    """Error raised when a resource type already exists."""
    def __init__(self, resource_type):
        super(ResourceTypeAlreadyExists, self).__init__(
            "Resource type %s already exists" % resource_type)
        self.resource_type = resource_type


class ResourceAttributeError(IndexerException, AttributeError):
    """Error raised when an attribute does not exist for a resource type."""
    def __init__(self, resource, attribute):
        super(ResourceAttributeError, self).__init__(
            "Resource type %s has no %s attribute" % (resource, attribute))
        self.resource = resource
        self.attribute = attribute


class ResourceValueError(IndexerException, ValueError):
    """Error raised when an attribute value is invalid for a resource type."""
    def __init__(self, resource_type, attribute, value):
        super(ResourceValueError, self).__init__(
            "Value %s for attribute %s on resource type %s is invalid"
            % (value, attribute, resource_type))
        self.resource_type = resource_type
        self.attribute = attribute
        self.value = value


class ArchivePolicyAlreadyExists(IndexerException):
    """Error raised when an archive policy already exists."""
    def __init__(self, name):
        super(ArchivePolicyAlreadyExists, self).__init__(
            "Archive policy %s already exists" % name)
        self.name = name


class ArchivePolicyRuleAlreadyExists(IndexerException):
    """Error raised when an archive policy rule already exists."""
    def __init__(self, name):
        super(ArchivePolicyRuleAlreadyExists, self).__init__(
            "Archive policy rule %s already exists" % name)
        self.name = name


class QueryError(IndexerException):
    def __init__(self):
        super(QueryError, self).__init__("Unable to parse this query")


class QueryValueError(QueryError, ValueError):
    def __init__(self, v, f):
        super(QueryError, self).__init__("Invalid value: `%s' for field `%s'"
                                         % (v, f))


class QueryInvalidOperator(QueryError):
    def __init__(self, op):
        self.op = op
        super(QueryError, self).__init__("Unknown operator `%s'" % op)


class QueryAttributeError(QueryError, ResourceAttributeError):
    def __init__(self, resource, attribute):
        ResourceAttributeError.__init__(self, resource, attribute)


class InvalidPagination(IndexerException):
    """Error raised when a resource does not exist."""
    def __init__(self, reason):
        self.reason = reason
        super(InvalidPagination, self).__init__(
            "Invalid pagination: `%s'" % reason)


class IndexerDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def disconnect():
        pass

    @staticmethod
    def upgrade(nocreate=False):
        pass

    @staticmethod
    def get_resource(resource_type, resource_id, with_metrics=False):
        """Get a resource from the indexer.

        :param resource_type: The type of the resource to look for.
        :param resource_id: The UUID of the resource.
        :param with_metrics: Whether to include metrics information.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def list_resources(resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False,
                       limit=None,
                       marker=None,
                       sorts=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_archive_policies():
        raise exceptions.NotImplementedError

    @staticmethod
    def get_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def update_archive_policy(name, ap_items):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_archive_policy_rule(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_archive_policy_rules():
        raise exceptions.NotImplementedError

    @staticmethod
    def create_archive_policy_rule(name, metric_pattern, archive_policy_name):
        raise exceptions.NotImplementedError

    @staticmethod
    def update_archive_policy_rule(name, new_name):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_archive_policy_rule(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_metric(id, creator,
                      archive_policy_name, name=None, unit=None,
                      resource_id=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_metrics(details=False, status='active',
                     limit=None, marker=None, sorts=None,
                     attribute_filter=None, policy_filter=None,
                     resource_policy_filter=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_archive_policy(archive_policy):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_resource(resource_type, id, creator,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def update_resource(resource_type, resource_id, ended_at=_marker,
                        metrics=_marker,
                        append_metrics=False,
                        create_revision=True,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_resource(uuid):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_resources(resource_type='generic',
                         attribute_filter=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_metric(id):
        raise exceptions.NotImplementedError

    @staticmethod
    def expunge_metric(id):
        raise exceptions.NotImplementedError

    def get_archive_policy_for_metric(self, metric_name):
        """Helper to get the archive policy according archive policy rules."""
        rules = self.list_archive_policy_rules()
        for rule in rules:
            if fnmatch.fnmatch(metric_name or "", rule.metric_pattern):
                return self.get_archive_policy(rule.archive_policy_name)
        raise NoArchivePolicyRuleMatch(metric_name)

    @staticmethod
    def create_resource_type(resource_type):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_resource_type(name):
        """Get a resource type from the indexer.

        :param name: name of the resource type
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def list_resource_types(attribute_filter=None,
                            limit=None,
                            marker=None,
                            sorts=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_resource_attributes_schemas():
        raise exceptions.NotImplementedError

    @staticmethod
    def get_resource_type_schema():
        raise exceptions.NotImplementedError
