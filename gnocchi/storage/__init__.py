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
import random
import uuid

from oslo.config import cfg
from stevedore import driver
from tooz import coordination

# TODO(eglynn): figure out how to accommodate multi-valued aggregation
#               methods, where there is no longer just a single aggregate
#               value to be stored per-period (e.g. ohlc)
AGGREGATION_TYPES = ('mean', 'sum', 'last', 'max', 'min',
                     'std', 'median', 'first')


ArchivePolicy = collections.namedtuple('ArchivePolicy',
                                       ['granularity', 'points'])


# TODO(jd) Store these policy using the driver and export a CRUD API so we
# can use with REST
ARCHIVE_POLICIES = {
    'low': [
        # 5 minutes resolution for an hour
        ArchivePolicy(300, 12),
        # 1 hour resolution for a day
        ArchivePolicy(3600, 24),
        # 1 day resolution for a month
        ArchivePolicy(3600 * 24, 30),
    ],
    'medium': [
        # 1 minute resolution for an hour
        ArchivePolicy(60, 60),
        # 1 hour resolution for a week
        ArchivePolicy(3600, 7 * 24),
        # 1 day resolution for a year
        ArchivePolicy(3600 * 24, 365),
    ],
    'high': [
        # 1 second resolution for a day
        ArchivePolicy(1, 3600 * 24),
        # 1 minute resolution for a month
        ArchivePolicy(60, 60 * 24 * 30),
        # 1 hour resolution for a year
        ArchivePolicy(3600, 365 * 24),
    ],
}

OPTS = [
    cfg.StrOpt('driver',
               default='swift',
               help='Storage driver to use'),
    cfg.StrOpt('coordination_url',
               help='Coordination driver URL',
               default='memcached://'),
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


class CoordinatorMixin(object):
    def _init_coordinator(self, url):
        self.aggregation_types = list(AGGREGATION_TYPES)
        self.coord = coordination.get_coordinator(
            url,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()
        # NOTE(jd) So this is a (smart?) optimization: since we're going to
        # lock for each of this aggregation type, if we are using running
        # Gnocchi with multiple processses, let's randomize what we iter
        # over so there are less chances we fight for the same lock!
        random.shuffle(self.aggregation_types)


class StorageDriver(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def create_entity(entity, archive_policy):
        """Create an entity.

        :param entity: The entity key.
        :param archive_policy: The archive policy to use.
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
