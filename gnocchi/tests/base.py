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
import errno
import functools
import json
import os
import uuid

import fixtures
from oslotest import base
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


class FakeRadosModule(object):
    class OpCtx(object):
        def __enter__(self):
            self.ops = []
            return self

        def __exit__(self, *args, **kwargs):
            pass

    WriteOpCtx = ReadOpCtx = OpCtx

    class OmapIterator(object):
        class OpRetCode(object):
            def __init__(self):
                self.ret = 0

            def __eq__(self, other):
                return self.ret == other

        def __init__(self, start_filter, prefix_filter, number):
            self.start_filter = start_filter
            self.prefix_filter = prefix_filter
            self.number = number
            self.data = {}
            self.op_ret = self.OpRetCode()

        def set_data(self, data):
            if not data:
                self.op_ret.ret = errno.ENOENT
            else:
                self.data = data

        def __iter__(self):
            # NOTE(sileht): we use only the prefix for now
            return ((k, v) for k, v in self.data.items()
                    if k.startswith(self.prefix_filter))

    LIBRADOS_OPERATION_BALANCE_READS = 1
    LIBRADOS_OPERATION_SKIPRWLOCKS = 16

    class ObjectNotFound(Exception):
        pass

    class ioctx(object):
        def __init__(self, kvs, kvs_xattrs, kvs_omaps):
            self.kvs = kvs
            self.kvs_xattrs = kvs_xattrs
            self.kvs_omaps = kvs_omaps
            self.librados = self
            self.io = self

        def __enter__(self):
            return self

        @staticmethod
        def __exit__(exc_type, exc_value, traceback):
            pass

        def _ensure_key_exists(self, key):
            if key not in self.kvs:
                self.kvs[key] = ""
                self.kvs_xattrs[key] = {}
                self.kvs_omaps[key] = {}

        @staticmethod
        def close():
            pass

        @staticmethod
        def _validate_key(name):
            if not isinstance(name, str):
                raise TypeError("key is not a 'str' object")

        def write_full(self, key, value):
            self._validate_key(key)
            self._ensure_key_exists(key)
            self.kvs[key] = value

        def write(self, key, value, offset):
            self._validate_key(key)
            try:
                current = self.kvs[key]
            except KeyError:
                current = b""
            if len(current) < offset:
                current += b'\x00' * (offset - len(current))
            self.kvs[key] = (
                current[:offset] + value + current[offset + len(value):]
            )

        def stat(self, key):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            else:
                return (1024, "timestamp")

        def read(self, key, length=8192, offset=0):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            else:
                return self.kvs[key][offset:offset+length]

        def operate_read_op(self, op, key, flag=0):
            for op in op.ops:
                op(key)

        def get_omap_vals(self, op, start_filter, prefix_filter, number):
            oi = FakeRadosModule.OmapIterator(start_filter, prefix_filter,
                                              number)
            op.ops.append(lambda oid: oi.set_data(self.kvs_omaps.get(oid)))
            return oi, oi.op_ret

        def operate_write_op(self, op, key, flags=0):
            for op in op.ops:
                op(key)

        def set_omap(self, op,  keys, values):
            def add(oid):
                self._ensure_key_exists(oid)
                omaps = self.kvs_omaps.setdefault(oid, {})
                omaps.update(dict(zip(keys, values)))
            op.ops.append(add)

        def remove_omap_keys(self, op,  keys):
            def rm(oid):
                for key in keys:
                    del self.kvs_omaps[oid][key]
            op.ops.append(rm)

        def get_xattrs(self, key):
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            return six.iteritems(self.kvs_xattrs.get(key, {}).copy())

        def set_xattr(self, key, attr, value):
            self._ensure_key_exists(key)
            xattrs = self.kvs_xattrs.setdefault(key, {})
            xattrs[attr] = value

        def rm_xattr(self, key, attr):
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            del self.kvs_xattrs[key][attr]

        def remove_object(self, key):
            self._validate_key(key)
            if key not in self.kvs:
                raise FakeRadosModule.ObjectNotFound
            del self.kvs[key]
            del self.kvs_xattrs[key]
            del self.kvs_omaps[key]

        def aio_remove(self, key):
            self._validate_key(key)
            self.kvs.pop(key, None)
            self.kvs_xattrs.pop(key, None)
            self.kvs_omaps.pop(key, None)

        @staticmethod
        def aio_flush():
            pass

    class FakeRados(object):
        def __init__(self, kvs, kvs_xattrs, kvs_omaps):
            self.kvs = kvs
            self.kvs_xattrs = kvs_xattrs
            self.kvs_omaps = kvs_omaps

        @staticmethod
        def connect():
            pass

        @staticmethod
        def shutdown():
            pass

        def open_ioctx(self, pool):
            return FakeRadosModule.ioctx(self.kvs, self.kvs_xattrs,
                                         self.kvs_omaps)

    def __init__(self):
        self.kvs = {}
        self.kvs_xattrs = {}
        self.kvs_omaps = {}

    def Rados(self, *args, **kwargs):
        return FakeRadosModule.FakeRados(self.kvs, self.kvs_xattrs,
                                         self.kvs_omaps)

    @staticmethod
    def run_in_thread(method, args):
        return method(*args)

    @staticmethod
    def make_ex(ret, reason):
        raise Exception(reason)


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


@six.add_metaclass(SkipNotImplementedMeta)
class TestCase(base.BaseTestCase):

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

    @classmethod
    def setUpClass(self):
        super(TestCase, self).setUpClass()
        self.conf = service.prepare_service([],
                                            default_config_files=[])
        self.conf.set_override('policy_file',
                               self.path_get('etc/gnocchi/policy.json'),
                               group="oslo_policy")

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

        self.conf.set_override(
            'driver',
            os.getenv("GNOCCHI_TEST_STORAGE_DRIVER", "file"),
            'storage')

    def setUp(self):
        super(TestCase, self).setUp()
        if swexc:
            self.useFixture(fixtures.MockPatch(
                'swiftclient.client.Connection',
                FakeSwiftClient))

        self.useFixture(fixtures.MockPatch('gnocchi.storage.ceph.rados',
                                           FakeRadosModule()))

        if self.conf.storage.driver == 'file':
            tempdir = self.useFixture(fixtures.TempDir())
            self.conf.set_override('file_basepath',
                                   tempdir.path,
                                   'storage')

        self.storage = storage.get_driver(self.conf)
        # NOTE(jd) Do not upgrade the storage. We don't really need the storage
        # upgrade for now, and the code that upgrade from pre-1.3
        # (TimeSerieArchive) uses a lot of parallel lock, which makes tooz
        # explodes because MySQL does not support that many connections in real
        # life.
        # self.storage.upgrade(self.index)

    def tearDown(self):
        self.index.disconnect()
        self.storage.stop()
        super(TestCase, self).tearDown()
