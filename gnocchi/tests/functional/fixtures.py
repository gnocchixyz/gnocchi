#
# Copyright 2015-2017 Red Hat. All Rights Reserved.
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
"""Fixtures for use with gabbi tests."""

from __future__ import absolute_import

import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from unittest import case
import uuid
import warnings

import fixtures
from gabbi import fixture
import numpy
from oslo_config import cfg
from oslo_middleware import cors
import sqlalchemy_utils
import yaml

from gnocchi import chef
from gnocchi.cli import metricd
from gnocchi import incoming
from gnocchi import indexer
from gnocchi.indexer import sqlalchemy
from gnocchi.rest import app
from gnocchi import service
from gnocchi import storage
from gnocchi.tests import base
from gnocchi.tests import utils

# NOTE(chdent): Hack to restore semblance of global configuration to
# pass to the WSGI app used per test suite. LOAD_APP_KWARGS are the olso
# configuration, and the pecan application configuration of
# which the critical part is a reference to the current indexer.
LOAD_APP_KWARGS = None


def setup_app():
    global LOAD_APP_KWARGS
    return app.load_app(**LOAD_APP_KWARGS)


class AssertNAN(yaml.YAMLObject):
    def __eq__(self, other):
        try:
            return numpy.isnan(other)
        except TypeError:
            return False


yaml.add_constructor(u'!AssertNAN', lambda loader, node: AssertNAN())


class ConfigFixture(fixture.GabbiFixture):
    """Establish the relevant configuration fixture, per test file.

    Each test file gets its own oslo config and its own indexer and storage
    instance. The indexer is based on the current database url. The storage
    uses a temporary directory.

    To use this fixture in a gabbit add::

        fixtures:
            - ConfigFixture
    """

    def __init__(self):
        self.conf = None
        self.tmp_dir = None

    def start_fixture(self):
        """Create necessary temp files and do the config dance."""
        global LOAD_APP_KWARGS

        if not os.getenv("GNOCCHI_TEST_DEBUG"):
            self.output = base.CaptureOutput()
            self.output.setUp()

        data_tmp_dir = tempfile.mkdtemp(prefix='gnocchi')

        if os.getenv("GABBI_LIVE"):
            dcf = None
        else:
            dcf = []
        conf = service.prepare_service([], conf=utils.prepare_conf(),
                                       default_config_files=dcf,
                                       logging_level=logging.DEBUG,
                                       skip_log_opts=True)

        py_root = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               '..', '..',))
        conf.set_override('paste_config',
                          os.path.join(py_root, 'rest', 'api-paste.ini'),
                          group="api")
        conf.set_override('policy_file',
                          os.path.join(py_root, 'rest', 'policy.json'),
                          group="oslo_policy")

        # NOTE(sileht): This is not concurrency safe, but only this tests file
        # deal with cors, so we are fine. set_override don't work because cors
        # group doesn't yet exists, and we the CORS middleware is created it
        # register the option and directly copy value of all configurations
        # options making impossible to override them properly...
        cfg.set_defaults(cors.CORS_OPTS, allowed_origin="http://foobar.com")

        self.conf = conf
        self.tmp_dir = data_tmp_dir

        if conf.indexer.url is None:
            raise case.SkipTest("No indexer configured")

        storage_driver = os.getenv("GNOCCHI_TEST_STORAGE_DRIVER", "file")

        conf.set_override('driver', storage_driver, 'storage')
        if conf.storage.driver == 'file':
            conf.set_override('file_basepath', data_tmp_dir, 'storage')
        elif conf.storage.driver == 'ceph':
            conf.set_override('ceph_conffile', os.getenv("CEPH_CONF"),
                              'storage')
            pool_name = uuid.uuid4().hex
            with open(os.devnull, 'w') as f:
                subprocess.call("rados -c %s mkpool %s" % (
                    os.getenv("CEPH_CONF"), pool_name), shell=True,
                    stdout=f, stderr=subprocess.STDOUT)
            conf.set_override('ceph_pool', pool_name, 'storage')
        elif conf.storage.driver == "s3":
            conf.set_override('s3_endpoint_url',
                              os.getenv("GNOCCHI_STORAGE_HTTP_URL"),
                              group="storage")
            conf.set_override('s3_access_key_id', "gnocchi", group="storage")
            conf.set_override('s3_secret_access_key', "anythingworks",
                              group="storage")
            conf.set_override("s3_bucket_prefix", str(uuid.uuid4())[:26],
                              "storage")
        elif conf.storage.driver == "swift":
            # NOTE(sileht): This fixture must start before any driver stuff
            swift_fixture = fixtures.MockPatch(
                'swiftclient.client.Connection',
                base.FakeSwiftClient)
            swift_fixture.setUp()

        # NOTE(jd) All of that is still very SQL centric but we only support
        # SQL for now so let's say it's good enough.
        conf.set_override(
            'url',
            sqlalchemy.SQLAlchemyIndexer._create_new_database(
                conf.indexer.url),
            'indexer')

        index = indexer.get_driver(conf)
        index.upgrade()

        # Set pagination to a testable value
        conf.set_override('max_limit', 7, 'api')

        self.index = index

        self.coord = metricd.get_coordinator_and_start(str(uuid.uuid4()),
                                                       conf.coordination_url)
        s = storage.get_driver(conf)
        i = incoming.get_driver(conf)

        if conf.storage.driver == 'redis':
            # Create one prefix per test
            s.STORAGE_PREFIX = str(uuid.uuid4()).encode()

        if conf.incoming.driver == 'redis':
            i.SACK_NAME_FORMAT = (
                str(uuid.uuid4()) + incoming.IncomingDriver.SACK_NAME_FORMAT
            )

        self.fixtures = [
            fixtures.MockPatch("gnocchi.storage.get_driver",
                               return_value=s),
            fixtures.MockPatch("gnocchi.incoming.get_driver",
                               return_value=i),
            fixtures.MockPatch("gnocchi.indexer.get_driver",
                               return_value=self.index),
            fixtures.MockPatch(
                "gnocchi.cli.metricd.get_coordinator_and_start",
                return_value=self.coord),
        ]
        for f in self.fixtures:
            f.setUp()

        if conf.storage.driver == 'swift':
            self.fixtures.append(swift_fixture)

        LOAD_APP_KWARGS = {
            'conf': conf,
        }

        s.upgrade()
        i.upgrade(128)

        # start up a thread to async process measures
        self.metricd_thread = MetricdThread(chef.Chef(self.coord, i, index, s))
        self.metricd_thread.start()

    def stop_fixture(self):
        """Clean up the config fixture and storage artifacts."""

        if hasattr(self, 'metricd_thread'):
            self.metricd_thread.stop()
            self.metricd_thread.join()

        if hasattr(self, 'fixtures'):
            for f in reversed(self.fixtures):
                f.cleanUp()

        if hasattr(self, 'index'):
            self.index.disconnect()

        # Swallow noise from missing tables when dropping
        # database.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore',
                                    module='sqlalchemy.engine.default')
            sqlalchemy_utils.drop_database(self.conf.indexer.url)

        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)

        if hasattr(self, 'coord'):
            self.coord.stop()

        self.conf.reset()
        if not os.getenv("GNOCCHI_TEST_DEBUG"):
            self.output.cleanUp()


class MetricdThread(threading.Thread):
    """Run metricd in a naive thread to process measures."""

    def __init__(self, chef, name='metricd'):
        super(MetricdThread, self).__init__(name=name)
        self.chef = chef
        self.flag = True

    def run(self):
        while self.flag:
            for sack in self.chef.incoming.iter_sacks():
                self.chef.process_new_measures_for_sack(sack, blocking=True)
            time.sleep(0.1)

    def stop(self):
        self.flag = False
