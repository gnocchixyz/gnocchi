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


class EntityAlreadyExists(Exception):
    """Error raised when an entity already exists."""
    def __init__(self, entity):
        super(EntityAlreadyExists, self).__init__("Entity %s already exists" %
                                                  entity)
        self.entity = entity


class IndexerDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def upgrade():
        pass

    @staticmethod
    def create_resource(uuid, entities=None):
        raise NotImplementedError

    @staticmethod
    def update_resource(uuid, entities=None):
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
