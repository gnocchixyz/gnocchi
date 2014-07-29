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

import fixtures
from oslo.config import fixture as config_fixture
from oslotest import mockpatch
import six
from swiftclient import exceptions as swexc
import testscenarios
import testtools
from testtools import testcase

from gnocchi import indexer
from gnocchi.openstack.common import lockutils
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
        except AssertionError:
            raise
        # FIXME(jd) Some Python code that is not our own can raise this, and
        # therefore skip a test without us knowing, which is a bad idea. We
        # need to define our own NotImplementedError.
        except NotImplementedError as e:
            raise testcase.TestSkipped(six.text_type(e))
    return skip_if_not_implemented


class FakeSwiftClient(object):
    def __init__(self, *args, **kwargs):
        self.kvs = {}

    def put_container(self, container):
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


load_tests = testscenarios.load_tests_apply_scenarios


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(testtools.TestCase, testscenarios.TestWithScenarios):

    indexer_backends = [
        ('null', dict(indexer_engine='null')),
        ('postgresql', dict(indexer_engine='sqlalchemy',
                            db_url=os.environ.get("GNOCCHI_TEST_PGSQL_URL"))),
        ('mysql', dict(indexer_engine='sqlalchemy',
                       db_url=os.environ.get("GNOCCHI_TEST_MYSQL_URL"))),
    ]

    storage_backends = [
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
            raise NotImplementedError

    def setUp(self):
        super(TestCase, self).setUp()
        self.conf = self.useFixture(config_fixture.Config()).conf
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
        with lockutils.lock("gnocchi-tests-db-lock", external=True):
            self.index.upgrade()

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
