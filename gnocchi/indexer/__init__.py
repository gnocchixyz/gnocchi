# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
from oslo.config import cfg
from stevedore import driver

from gnocchi import exceptions

OPTS = [
    cfg.StrOpt('driver',
               default='sqlalchemy',
               help='Indexer driver to use'),
]

cfg.CONF.register_opts(OPTS, group="indexer")


_marker = object()


def _get_driver(name, conf):
    """Return the driver named name.

    :param name: The name of the driver.
    :param conf: The conf to pass to the driver.
    """
    d = driver.DriverManager('gnocchi.indexer',
                             name).driver
    return d(conf)


def get_driver(conf):
    """Return the configured driver."""
    return _get_driver(conf.indexer.driver,
                       conf)


class UnknownResourceType(Exception):
    """Error raised when the resource type is unknown."""
    def __init__(self, type):
        super(UnknownResourceType, self).__init__(
            "Resource type %s is unknown" % type)
        self.type = type


class NoSuchMetric(Exception):
    """Error raised when a metric does not exist."""
    def __init__(self, metric):
        super(NoSuchMetric, self).__init__("Metric %s does not exist" %
                                           str(metric))
        self.metric = metric


class NoSuchResource(Exception):
    """Error raised when a resource does not exist."""
    def __init__(self, resource):
        super(NoSuchResource, self).__init__("Resource %s does not exist" %
                                             str(resource))
        self.resource = resource


class NoSuchArchivePolicy(Exception):
    """Error raised when an archive policy does not exist."""
    def __init__(self, archive_policy):
        super(NoSuchArchivePolicy, self).__init__(
            "Archive policy %s does not exist" %
            str(archive_policy))
        self.archive_policy = archive_policy


class ArchivePolicyInUse(Exception):
    """Error raised when an archive policy is still being used."""
    def __init__(self, archive_policy):
        super(ArchivePolicyInUse, self).__init__(
            "Archive policy %s is still in use" % archive_policy)
        self.archive_policy = archive_policy


class NamedMetricAlreadyExists(Exception):
    """Error raised when a named metric already exists."""
    def __init__(self, metric):
        super(NamedMetricAlreadyExists, self).__init__(
            "Named metric %s already exists" % metric)
        self.metric = metric


class ResourceAlreadyExists(Exception):
    """Error raised when a resource already exists."""
    def __init__(self, resource):
        super(ResourceAlreadyExists, self).__init__(
            "Resource %s already exists" % resource)
        self.resource = resource


class ResourceAttributeError(AttributeError):
    """Error raised when an attribute does not exist for a resource type."""
    def __init__(self, resource, attribute):
        super(ResourceAttributeError, self).__init__(
            "Resource %s has no %s attribute" % (resource, attribute))
        self.resource = resource,
        self.attribute = attribute


class ResourceValueError(ValueError):
    """Error raised when an attribute value is invalid for a resource type."""
    def __init__(self, resource_type, attribute, value):
        super(ResourceValueError, self).__init__(
            "Value %s for attribute %s on resource type %s is invalid"
            % (value, attribute, resource_type))
        self.resource_type = resource_type
        self.attribute = attribute
        self.value = value


class ArchivePolicyAlreadyExists(Exception):
    """Error raised when an archive policy already exists."""
    def __init__(self, name):
        super(ArchivePolicyAlreadyExists, self).__init__(
            "Archive policy %s already exists" % name)
        self.name = name


class IndexerDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def connect():
        pass

    @staticmethod
    def disconnect():
        pass

    @staticmethod
    def upgrade():
        pass

    @staticmethod
    def get_resource(resource_type, uuid, with_metrics=False):
        """Get a resource from the indexer.

        :param resource_type: The type of the resource to look for.
        :param uuid: The UUID of the resource.
        :param with_metrics: Whether to include metrics information.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def list_resources(resource_type='generic', started_after=None,
                       ended_before=None,
                       attributes_filter=None,
                       details=False):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_archive_policies():
        raise exceptions.NotImplementedError

    @staticmethod
    def get_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_archive_policy(name):
        raise exceptions.NotImplementedError

    @staticmethod
    def get_metrics(uuids, details=False):
        """Get metrics informations from the indexer.

        :param uuids: A list of metric UUID.
        :param details: Whether to return metrics details.
        """
        raise exceptions.NotImplementedError

    @staticmethod
    def create_metric(id, created_by_user_id, created_by_project_id,
                      archive_policy_name, name=None, resource_id=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def list_metrics(user_id=None, project_id=None):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_archive_policy(name, back_window, definition):
        raise exceptions.NotImplementedError

    @staticmethod
    def create_resource(resource_type, id, user_id, project_id,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def update_resource(resource_type, uuid, ended_at=_marker,
                        metrics=_marker,
                        append_metrics=False,
                        **kwargs):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_resource(uuid):
        raise exceptions.NotImplementedError

    @staticmethod
    def delete_metric(id):
        raise exceptions.NotImplementedError
