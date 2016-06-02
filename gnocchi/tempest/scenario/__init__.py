#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from __future__ import absolute_import

import os
import unittest

from gabbi import driver
import six.moves.urllib.parse as urlparse
from tempest import config
import tempest.test

CONF = config.CONF


class GnocchiGabbiTest(tempest.test.BaseTestCase):
    credentials = ['admin']

    @classmethod
    def skip_checks(cls):
        super(GnocchiGabbiTest, cls).skip_checks()
        if not CONF.service_available.gnocchi:
            raise cls.skipException("Gnocchi support is required")

    @classmethod
    def resource_setup(cls):
        super(GnocchiGabbiTest, cls).resource_setup()

        url = cls.os_admin.auth_provider.base_url(
            {'service':  CONF.metric.catalog_type,
             'endpoint_type': CONF.metric.endpoint_type})
        token = cls.os_admin.auth_provider.get_token()

        parsed_url = urlparse.urlsplit(url)
        prefix = parsed_url.path.rstrip('/')  # turn it into a prefix
        if parsed_url.scheme == 'https':
            port = 443
            require_ssl = True
        else:
            port = 80
            require_ssl = False
        host = parsed_url.hostname
        if parsed_url.port:
            port = parsed_url.port

        test_dir = os.path.join(os.path.dirname(__file__), '..', '..',
                                'tests', 'gabbi', 'gabbits-live')
        cls.tests = driver.build_tests(
            test_dir, unittest.TestLoader(),
            host=host, port=port, prefix=prefix,
            test_loader_name='tempest.scenario.gnocchi.test',
            require_ssl=require_ssl)

        os.environ["GNOCCHI_SERVICE_TOKEN"] = token

    @classmethod
    def clear_credentials(cls):
        # FIXME(sileht): We don't want the token to be invalided, but
        # for some obcurs reason, clear_credentials is called before/during run
        # So, make the one used by tearDropClass a dump, and call it manually
        # in run()
        pass

    def run(self, result=None):
        self.setUp()
        try:
            self.tests.run(result)
        finally:
            super(GnocchiGabbiTest, self).clear_credentials()
            self.tearDown()

    def test_fake(self):
        # NOTE(sileht): A fake test is needed to have the class loaded
        # by the test runner
        pass
