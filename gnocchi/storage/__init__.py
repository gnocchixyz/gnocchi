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
import collections

from oslo.config import cfg
from stevedore import driver

#TODO(eglynn): figure out how to accommodate multi-valued aggregation
#              methods, where there is no longer just a single aggregate
#              value to be stored per-period (e.g. ohlc)
AGGREGATION_TYPES = ('mean', 'sum', 'last', 'max', 'min',
                     'std', 'median', 'first')


OPTS = [
    cfg.StrOpt('driver',
               default='swift',
               help='Storage driver to use'),
    cfg.IntOpt('compression_level',
               default=1,
               help='Storage compression level, if supported.'),
]

cfg.CONF.register_opts(OPTS, group="storage")


Measure = collections.namedtuple('Measure', ['timestamp', 'value'])


class EntityDoesNotExist(Exception):
    """Error raised when this entity does not exist."""

    def __init__(self, entity):
        self.entity = entity
        super(EntityDoesNotExist, self).__init__(
            "Entity %s does not exist" % entity)


class EntityAlreadyExists(Exception):
    """Error raised when this entity already exists."""

    def __init__(self, entity):
        self.entity = entity
        super(EntityAlreadyExists, self).__init__(
            "Entity %s already exists" % entity)


def _get_driver(name, conf):
    """Return the driver named name.

    :param name: The name of the driver.
    :param conf: The conf to pass to the driver.
    """
    d = driver.DriverManager('gnocchi.storage',
                             name).driver
    return d(conf)


def get_driver(conf):
    """Return the configured driver."""
    return _get_driver(conf.storage.driver,
                       conf.storage)


class StorageDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def create_entity(entity, archive):
        """Create an entity.

        :param entity: The entity key.
        :param archive: The archive configuration to use.
                        A list of (seconds, points) that indicates how many
                        points to keep every seconds interval in archives.
        """
        raise NotImplementedError

    @staticmethod
    def add_measures(entity, measures):
        """Add a measure to an entity.

        :param entity: The entity measured.
        :param measures: The actual measures.
        """
        raise NotImplementedError

    @staticmethod
    def get_measures(entity, from_timestamp=None, to_timestamp=None,
                     aggregation='average'):
        """Add a measure to an entity.

        :param entity: The entity measured.
        :param from timestamp: The timestamp to get the measure from.
        :param to timestamp: The timestamp to get the measure to.
        :param aggregation: The type of aggregation to retrieve.
        """
        raise NotImplementedError
