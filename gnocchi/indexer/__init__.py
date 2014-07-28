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


class NoSuchEntity(Exception):
    """Error raised when an entitiy does not exist."""
    def __init__(self, entity):
        super(NoSuchEntity, self).__init__("Entity %s does not exist" %
                                           str(entity))
        self.entity = entity


class NoSuchResource(Exception):
    """Error raised when a resource does not exist."""
    def __init__(self, resource):
        super(NoSuchResource, self).__init__("Resource %s does not exist" %
                                             str(resource))
        self.resource = resource


class ResourceAlreadyExists(Exception):
    """Error raised when a resource already exists."""
    def __init__(self, resource):
        super(ResourceAlreadyExists, self).__init__(
            "Resource %s already exists" % resource)
        self.resource = resource


class EntityAlreadyExists(Exception):
    """Error raised when an entity already exists."""
    def __init__(self, entity):
        super(EntityAlreadyExists, self).__init__("Entity %s already exists" %
                                                  entity)
        self.entity = entity


class ResourceAttributeError(AttributeError):
    """Error raised when an attribute does not exist for a resource type."""
    def __init__(self, resource, attribute):
        super(ResourceAttributeError, self).__init__(
            "Resource %s has no %s attribute" % (resource, attribute))
        self.resource = resource,
        self.attribute = attribute


class IndexerDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def connect():
        pass

    @staticmethod
    def upgrade():
        pass

    @staticmethod
    def get_resource(resource_type, uuid):
        raise NotImplementedError

    @staticmethod
    def list_resources(resource_type='generic', started_after=None,
                       ended_before=None,
                       attributes_filter=None,
                       details=False):
        raise NotImplementedError

    @staticmethod
    def create_resource(resource_type, id, user_id, project_id,
                        started_at=None, ended_at=None, entities=None,
                        **kwargs):
        raise NotImplementedError

    @staticmethod
    def update_resource(resource_type, uuid, ended_at=_marker,
                        entities=_marker,
                        **kwargs):
        raise NotImplementedError

    @staticmethod
    def delete_resource(uuid):
        raise NotImplementedError

    @staticmethod
    def create_entity(id):
        raise NotImplementedError

    @staticmethod
    def delete_entity(id):
        raise NotImplementedError
