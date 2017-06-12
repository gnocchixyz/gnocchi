# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2016 eNovance
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
import json
import os
import subprocess
import threading
import uuid

import daiquiri
import fixtures
import six
from six.moves.urllib.parse import unquote
try:
    from swiftclient import exceptions as swexc
except ImportError:
    swexc = None
from testtools import testcase
from tooz import coordination

from gnocchi import archive_policy
from gnocchi import exceptions
from gnocchi import indexer
from gnocchi import service
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

    def get_container(self, container, delimiter=None,
                      path=None, full_listing=False, limit=None):
        try:
            container = self.kvs[container]
        except KeyError:
            raise swexc.ClientException("No such container",
                                        http_status=404)

        files = []
        directories = set()
        for k, v in six.iteritems(container.copy()):
            if path and not k.startswith(path):
                continue

            if delimiter is not None and delimiter in k:
                dirname = k.split(delimiter, 1)[0]
                if dirname not in directories:
                    directories.add(dirname)
                    files.append({'subdir': dirname + delimiter})
            else:
                files.append({'bytes': len(v),
                              'last_modified': None,
                              'hash': None,
                              'name': k,
                              'content_type': None})

        if full_listing:
            end = None
        elif limit:
            end = limit
        else:
            # In truth, it's 10000, but 1 is enough to make sure our test fails
            # otherwise.
            end = 1

        return ({'x-container-object-count': len(container.keys())},
                (files + list(directories))[:end])

    def put_object(self, container, key, obj):
        if hasattr(obj, "seek"):
            obj.seek(0)
            obj = obj.read()
            # TODO(jd) Maybe we should reset the seek(), but well…
        try:
            self.kvs[container][key] = obj
        except KeyError:
            raise swexc.ClientException("No such container",
                                        http_status=404)

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
        if self.kvs[container]:
            raise swexc.ClientException("Container not empty",
                                        http_status=409)
        del self.kvs[container]

    def head_container(self, container):
        if container not in self.kvs:
            raise swexc.ClientException("No such container",
                                        http_status=404)

    def post_account(self, headers, query_string=None, data=None,
                     response_dict=None):
        if query_string == 'bulk-delete':
            resp = {'Response Status': '200 OK',
                    'Response Body': '',
                    'Number Deleted': 0,
                    'Number Not Found': 0}
            if response_dict is not None:
                response_dict['status'] = 200
            if data:
                for path in data.splitlines():
                    try:
                        __, container, obj = (unquote(path.decode('utf8'))
                                              .split('/', 2))
                        del self.kvs[container][obj]
                        resp['Number Deleted'] += 1
                    except KeyError:
                        resp['Number Not Found'] += 1
            return {}, json.dumps(resp).encode('utf-8')

        if response_dict is not None:
            response_dict['status'] = 204

        return {}, None


class CaptureOutput(fixtures.Fixture):
    """Optionally capture the output streams.

    .. py:attribute:: stdout

       The ``stream`` attribute from a :class:`StringStream` instance
       replacing stdout.

    .. py:attribute:: stderr

       The ``stream`` attribute from a :class:`StringStream` instance
       replacing stderr.

    """

    def setUp(self):
        super(CaptureOutput, self).setUp()
        self._stdout_fixture = fixtures.StringStream('stdout')
        self.stdout = self.useFixture(self._stdout_fixture).stream
        self.useFixture(fixtures.MonkeyPatch('sys.stdout', self.stdout))
        self._stderr_fixture = fixtures.StringStream('stderr')
        self.stderr = self.useFixture(self._stderr_fixture).stream
        self.useFixture(fixtures.MonkeyPatch('sys.stderr', self.stderr))


class BaseTestCase(testcase.TestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        if not os.getenv("GNOCCHI_TEST_DEBUG"):
            self.useFixture(CaptureOutput())


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(BaseTestCase):

    REDIS_DB_INDEX = 0
    REDIS_DB_LOCK = threading.Lock()

    ARCHIVE_POLICIES = {
        'no_granularity_match': archive_policy.ArchivePolicy(
            "no_granularity_match",
            0, [
                # 2 second resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=2, points=3600 * 24),
            ],
        ),
        'low': archive_policy.ArchivePolicy(
            "low", 0, [
                # 5 minutes resolution for an hour
                archive_policy.ArchivePolicyItem(
                    granularity=300, points=12),
                # 1 hour resolution for a day
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=24),
                # 1 day resolution for a month
                archive_policy.ArchivePolicyItem(
                    granularity=3600 * 24, points=30),
            ],
        ),
        'medium': archive_policy.ArchivePolicy(
            "medium", 0, [
                # 1 minute resolution for an day
                archive_policy.ArchivePolicyItem(
                    granularity=60, points=60 * 24),
                # 1 hour resolution for a week
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=7 * 24),
                # 1 day resolution for a year
                archive_policy.ArchivePolicyItem(
                    granularity=3600 * 24, points=365),
            ],
        ),
        'high': archive_policy.ArchivePolicy(
            "high", 0, [
                # 1 second resolution for an hour
                archive_policy.ArchivePolicyItem(
                    granularity=1, points=3600),
                # 1 minute resolution for a week
                archive_policy.ArchivePolicyItem(
                    granularity=60, points=60 * 24 * 7),
                # 1 hour resolution for a year
                archive_policy.ArchivePolicyItem(
                    granularity=3600, points=365 * 24),
            ],
        ),
    }

    @classmethod
    def setUpClass(self):
        super(TestCase, self).setUpClass()
        self.conf = service.prepare_service([],
                                            default_config_files=[])
        if not os.getenv("GNOCCHI_TEST_DEBUG"):
            daiquiri.setup(outputs=[])

        py_root = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               '..',))
        self.conf.set_override('paste_config',
                               os.path.join(py_root, 'rest', 'api-paste.ini'),
                               group="api")
        self.conf.set_override('policy_file',
                               os.path.join(py_root, 'rest', 'policy.json'),
                               group="oslo_policy")

        # NOTE(jd) This allows to test S3 on AWS
        if not os.getenv("AWS_ACCESS_KEY_ID"):
            self.conf.set_override('s3_endpoint_url',
                                   os.getenv("GNOCCHI_STORAGE_HTTP_URL"),
                                   group="storage")
            self.conf.set_override('s3_access_key_id', "gnocchi",
                                   group="storage")
            self.conf.set_override('s3_secret_access_key', "anythingworks",
                                   group="storage")

        self.index = indexer.get_driver(self.conf)
        self.index.connect()

        # NOTE(jd) So, some driver, at least SQLAlchemy, can't create all
        # their tables in a single transaction even with the
        # checkfirst=True, so what we do here is we force the upgrade code
        # path to be sequential to avoid race conditions as the tests run
        # in parallel.
        self.coord = coordination.get_coordinator(
            self.conf.storage.coordination_url,
            str(uuid.uuid4()).encode('ascii'))

        self.coord.start(start_heart=True)

        with self.coord.get_lock(b"gnocchi-tests-db-lock"):
            self.index.upgrade()

        self.coord.stop()

        self.archive_policies = self.ARCHIVE_POLICIES.copy()
        for name, ap in six.iteritems(self.archive_policies):
            # Create basic archive policies
            try:
                self.index.create_archive_policy(ap)
            except indexer.ArchivePolicyAlreadyExists:
                pass

        storage_driver = os.getenv("GNOCCHI_TEST_STORAGE_DRIVER", "file")
        self.conf.set_override('driver', storage_driver, 'storage')
        if storage_driver == 'ceph':
            self.conf.set_override('ceph_conffile',
                                   os.getenv("CEPH_CONF"),
                                   'storage')

    def setUp(self):
        super(TestCase, self).setUp()
        if swexc:
            self.useFixture(fixtures.MockPatch(
                'swiftclient.client.Connection',
                FakeSwiftClient))

        if self.conf.storage.driver == 'file':
            tempdir = self.useFixture(fixtures.TempDir())
            self.conf.set_override('file_basepath',
                                   tempdir.path,
                                   'storage')
        elif self.conf.storage.driver == 'ceph':
            pool_name = uuid.uuid4().hex
            subprocess.call("rados -c %s mkpool %s" % (
                os.getenv("CEPH_CONF"), pool_name), shell=True)
            self.conf.set_override('ceph_pool', pool_name, 'storage')

        # Override the bucket prefix to be unique to avoid concurrent access
        # with any other test
        self.conf.set_override("s3_bucket_prefix", str(uuid.uuid4())[:26],
                               "storage")

        self.storage = storage.get_driver(self.conf)

        if self.conf.storage.driver == 'redis':
            # Create one prefix per test
            self.storage.STORAGE_PREFIX = str(uuid.uuid4())
            self.storage.incoming.SACK_PREFIX = str(uuid.uuid4())

        self.storage.upgrade(128)

    def tearDown(self):
        self.index.disconnect()
        self.storage.stop()
        super(TestCase, self).tearDown()
