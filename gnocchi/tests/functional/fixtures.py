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
import threading
import time
from unittest import case
import warnings

from gabbi import fixture
from oslo_config import cfg
from oslo_middleware import cors
import sqlalchemy_utils

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy
from gnocchi.rest import app
from gnocchi import service
from gnocchi import storage


# NOTE(chdent): Hack to restore semblance of global configuration to
# pass to the WSGI app used per test suite. LOAD_APP_KWARGS are the olso
# configuration, and the pecan application configuration of
# which the critical part is a reference to the current indexer.
LOAD_APP_KWARGS = None


def setup_app():
    global LOAD_APP_KWARGS
    return app.load_app(**LOAD_APP_KWARGS)


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

        data_tmp_dir = tempfile.mkdtemp(prefix='gnocchi')

        if os.getenv("GABBI_LIVE"):
            dcf = None
        else:
            dcf = []
        conf = service.prepare_service([],
                                       default_config_files=dcf)

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

        # Use the presence of DEVSTACK_GATE_TEMPEST as a semaphore
        # to signal we are not in a gate driven functional test
        # and thus should override conf settings.
        if 'DEVSTACK_GATE_TEMPEST' not in os.environ:
            conf.set_override('driver', 'file', 'storage')
            conf.set_override('file_basepath', data_tmp_dir, 'storage')

        # NOTE(jd) All of that is still very SQL centric but we only support
        # SQL for now so let's say it's good enough.
        conf.set_override(
            'url',
            sqlalchemy.SQLAlchemyIndexer._create_new_database(
                conf.indexer.url),
            'indexer')

        index = indexer.get_driver(conf)
        index.connect()
        index.upgrade(create_legacy_resource_types=True)

        # Set pagination to a testable value
        conf.set_override('max_limit', 7, 'api')
        # Those tests uses noauth mode
        # TODO(jd) Rewrite them for basic
        conf.set_override("auth_mode", "noauth", 'api')

        self.index = index

        s = storage.get_driver(conf)
        s.upgrade(index)

        LOAD_APP_KWARGS = {
            'storage': s,
            'indexer': index,
            'conf': conf,
        }

        # start up a thread to async process measures
        self.metricd_thread = MetricdThread(index, s)
        self.metricd_thread.start()

    def stop_fixture(self):
        """Clean up the config fixture and storage artifacts."""
        if hasattr(self, 'metricd_thread'):
            self.metricd_thread.stop()
            self.metricd_thread.join()

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

        self.conf.reset()


class MetricdThread(threading.Thread):
    """Run metricd in a naive thread to process measures."""

    def __init__(self, index, storer, name='metricd'):
        super(MetricdThread, self).__init__(name=name)
        self.index = index
        self.storage = storer
        self.flag = True

    def run(self):
        incoming = self.storage.incoming
        while self.flag:
            metrics = incoming.list_metric_with_measures_to_process(
                None, None, full=True)
            self.storage.process_background_tasks(self.index, metrics)
            time.sleep(0.1)

    def stop(self):
        self.flag = False
