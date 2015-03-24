#
# Copyright 2015 Red Hat. All Rights Reserved.
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

import os
import shutil
import tempfile
from unittest import case
import uuid
import warnings

from gabbi import fixture
import sqlalchemy.engine.url as sqlalchemy_url
import sqlalchemy_utils

from gnocchi import indexer
from gnocchi.rest import app
from gnocchi import service


# NOTE(chdent): Hack to restore semblance of global conf to pass to
# the WSGI app used per test suite.
CONF = None


def setup_app():
    global CONF
    return app.setup_app(cfg=CONF)


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
        self.db_url = None
        self.tmp_dir = None

    def start_fixture(self):
        """Create necessary temp files and do the config dance."""

        global CONF

        data_tmp_dir = tempfile.mkdtemp(prefix='gnocchi')
        coordination_dir = os.path.join(data_tmp_dir, 'tooz')
        os.mkdir(coordination_dir)
        coordination_url = 'file://%s' % coordination_dir

        conf = service.prepare_service([])

        CONF = self.conf = conf
        self.tmp_dir = data_tmp_dir

        # Use the indexer set in the conf, unless we have set an
        # override via the environment.
        if 'GNOCCHI_TEST_INDEXER_URL' in os.environ:
            conf.set_override('url',
                              os.environ.get("GNOCCHI_TEST_INDEXER_URL"),
                              'indexer')

        # TODO(jd) It would be cool if Gabbi was able to use the null://
        # indexer, but this makes the API returns a lot of 501 error, which
        # Gabbi does not want to see, so let's just disable it.
        if conf.indexer.url is None or conf.indexer.url == "null://":
            raise case.SkipTest("No indexer configured")

        # Use the presence of DEVSTACK_GATE_TEMPEST as a semaphore
        # to signal we are not in a gate driven functional test
        # and thus should override conf settings.
        if 'DEVSTACK_GATE_TEMPEST' not in os.environ:
            conf.set_override('driver', 'file', 'storage')
            conf.set_override('coordination_url', coordination_url, 'storage')
            conf.set_override('policy_file',
                              os.path.abspath('etc/gnocchi/policy.json'),
                              group="oslo_policy")
            conf.set_override('file_basepath', data_tmp_dir, 'storage')

        # NOTE(jd) All of that is still very SQL centric but we only support
        # SQL for now so let's say it's good enough.
        url = sqlalchemy_url.make_url(conf.indexer.url)

        url.database = url.database + str(uuid.uuid4()).replace('-', '')
        db_url = str(url)
        conf.set_override('url', db_url, 'indexer')
        sqlalchemy_utils.create_database(db_url)

        index = indexer.get_driver(conf)
        index.connect()
        index.upgrade()

        conf.set_override('pecan_debug', False, 'api')

        # Turn off any middleware.
        conf.set_override('middlewares', [], 'api')

    def stop_fixture(self):
        """Clean up the config fixture and storage artifacts."""
        self.conf.reset()

        if not self.conf.indexer.url.startswith("null://"):
            # Swallow noise from missing tables when dropping
            # database.
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore',
                                        module='sqlalchemy.engine.default')
                sqlalchemy_utils.drop_database(self.conf.indexer.url)
        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)
