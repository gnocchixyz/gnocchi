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
import functools
import os
import uuid

import fixtures
from oslo.config import fixture as config_fixture
from oslotest import mockpatch
import six
from swiftclient import exceptions as swexc
import testscenarios
import testtools
from testtools import testcase
from tooz import coordination

from gnocchi import exceptions
from gnocchi import indexer
from gnocchi import storage


class SkipNotImplementedMeta(type):
    def __new__(cls, name, bases, local):
        for attr in local:
            value = local[attr]
            if callable(value) and (
                    attr.startswith('test_') or attr == 'setUp'):
                local[attr] = _skip_decorator(value)
        return type.__new__(cls, name, bases, local)


def _skip_decorator(func):
    @functools.wraps(func)
    def skip_if_not_implemented(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exceptions.NotImplementedError as e:
            raise testcase.TestSkipped(six.text_type(e))
    return skip_if_not_implemented


class FakeSwiftClient(object):
    def __init__(self, *args, **kwargs):
        self.kvs = {}

    def put_container(self, container, response_dict=None):
        if response_dict is not None:
            if container in self.kvs:
                response_dict['status'] = 204
            else:
                response_dict['status'] = 201
        self.kvs[container] = {}

    def put_object(self, container, key, obj):
        if hasattr(obj, "seek"):
            obj.seek(0)
            obj = obj.read()
            # TODO(jd) Maybe we should reset the seek(), but well…
        self.kvs[container][key] = obj

    def get_object(self, container, key):
        try:
            return {}, self.kvs[container][key]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_object(self, container, obj):
        try:
            del self.kvs[container][obj]
        except KeyError:
            raise swexc.ClientException("No such container/object",
                                        http_status=404)

    def delete_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)
        del self.kvs[container]

    def head_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(testtools.TestCase, testscenarios.TestWithScenarios):

    ARCHIVE_POLICIES = {
        'low': [
            # 5 minutes resolution for an hour
            {"granularity": 300, "points": 12},
            # 1 hour resolution for a day
            {"granularity": 3600, "points": 24},
            # 1 day resolution for a month
            {"granularity": 3600 * 24, "points": 30},
        ],
        'medium': [
            # 1 minute resolution for an hour
            {"granularity": 60, "points": 60},
            # 1 hour resolution for a week
            {"granularity": 3600, "points": 7 * 24},
            # 1 day resolution for a year
            {"granularity": 3600 * 24, "points": 365},
        ],
        'high': [
            # 1 second resolution for a day
            {"granularity": 1, "points": 3600 * 24},
            # 1 minute resolution for a month
            {"granularity": 60, "points": 60 * 24 * 30},
            # 1 hour resolution for a year
            {"granularity": 3600, "points": 365 * 24},
        ],
    }

    indexer_backends = [
        ('null', dict(indexer_engine='null')),
        ('postgresql', dict(indexer_engine='sqlalchemy',
                            db_url=os.environ.get("GNOCCHI_TEST_PGSQL_URL"))),
        ('mysql', dict(indexer_engine='sqlalchemy',
                       db_url=os.environ.get("GNOCCHI_TEST_MYSQL_URL"))),
    ]

    storage_backends = [
        ('null', dict(storage_engine='null')),
        ('swift', dict(storage_engine='swift')),
        ('file', dict(storage_engine='file')),
    ]

    scenarios = testscenarios.multiply_scenarios(storage_backends,
                                                 indexer_backends)

    def _pre_connect_sqlalchemy(self):
        self.conf.set_override('connection',
                               getattr(self, "db_url", "sqlite:///"),
                               'database')
        # No env var exported, no integration tests
        if self.conf.database.connection is None:
            raise testcase.TestSkipped("No database connection configured")

    @staticmethod
    def path_get(project_file=None):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..',
                                            )
                               )
        if project_file:
            return os.path.join(root, project_file)
        return root

    def setUp(self):
        super(TestCase, self).setUp()
        self.conf = self.useFixture(config_fixture.Config()).conf
        self.conf([], project='gnocchi')
        self.conf.import_opt('policy_file', 'gnocchi.openstack.common.policy')
        self.conf.set_override('policy_file',
                               self.path_get('etc/gnocchi/policy.json'))
        self.conf.import_opt('debug', 'gnocchi.openstack.common.log')
        self.conf.set_override('debug', True)

        self.conf.set_override('driver', self.indexer_engine, 'indexer')
        self.index = indexer.get_driver(self.conf)
        pre_connect_func = getattr(self, "_pre_connect_" + self.indexer_engine,
                                   None)
        if pre_connect_func:
            pre_connect_func()
        self.index.connect()

        # NOTE(jd) So, some driver, at least SQLAlchemy, can't create all
        # their tables in a single transaction even with the
        # checkfirst=True, so what we do here is we force the upgrade code
        # path to be sequential to avoid race conditions as the tests run in
        # parallel.
        self.coord = coordination.get_coordinator(
            "ipc://", str(uuid.uuid4()).encode('ascii'))

        with self.coord.get_lock(b"gnocchi-tests-db-lock"):
            self.index.upgrade()

        self.archive_policies = {}
        for name, definition in six.iteritems(self.ARCHIVE_POLICIES):
            # Create basic archive policies
            try:
                self.archive_policies[name] = self.index.create_archive_policy(
                    name=name,
                    definition=definition)['definition']
            except indexer.ArchivePolicyAlreadyExists:
                self.archive_policies[name] = self.index.get_archive_policy(
                    name)['definition']

        self.useFixture(mockpatch.Patch(
            'swiftclient.client.Connection',
            FakeSwiftClient))

        if self.storage_engine == 'file':
            self.conf.import_opt('file_basepath',
                                 'gnocchi.storage.file',
                                 group='storage')
            self.tempdir = self.useFixture(fixtures.TempDir())
            self.conf.set_override('file_basepath',
                                   self.tempdir.path,
                                   'storage')

        self.conf.set_override('driver', self.storage_engine, 'storage')
        self.storage = storage.get_driver(self.conf)

    def tearDown(self):
        self.index.disconnect()
        super(TestCase, self).tearDown()
