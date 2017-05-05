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

from gabbi import runner
from gabbi import suitemaker
from gabbi import utils
import six.moves.urllib.parse as urlparse
from tempest import config
import tempest.test

CONF = config.CONF

TEST_DIR = os.path.join(os.path.dirname(__file__), '..', '..',
                        'tests', 'functional_live', 'gabbits')


class GnocchiGabbiTest(tempest.test.BaseTestCase):
    credentials = ['admin']

    TIMEOUT_SCALING_FACTOR = 5

    @classmethod
    def skip_checks(cls):
        super(GnocchiGabbiTest, cls).skip_checks()
        if not CONF.service_available.gnocchi:
            raise cls.skipException("Gnocchi support is required")

    def _do_test(self, filename):
        token = self.os_admin.auth_provider.get_token()
        url = self.os_admin.auth_provider.base_url(
            {'service':  CONF.metric.catalog_type,
             'endpoint_type': CONF.metric.endpoint_type})

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

        os.environ["GNOCCHI_SERVICE_TOKEN"] = token
        os.environ["GNOCCHI_AUTHORIZATION"] = "not used"

        with file(os.path.join(TEST_DIR, filename)) as f:
            suite_dict = utils.load_yaml(f)
            suite_dict.setdefault('defaults', {})['ssl'] = require_ssl
            test_suite = suitemaker.test_suite_from_dict(
                loader=unittest.defaultTestLoader,
                test_base_name="gabbi",
                suite_dict=suite_dict,
                test_directory=TEST_DIR,
                host=host, port=port,
                fixture_module=None,
                intercept=None,
                prefix=prefix,
                handlers=runner.initialize_handlers([]),
                test_loader_name="tempest")

            # NOTE(sileht): We hide stdout/stderr and reraise the failure
            # manually, tempest will print it itself.
            with open(os.devnull, 'w') as stream:
                result = unittest.TextTestRunner(
                    stream=stream, verbosity=0, failfast=True,
                ).run(test_suite)

            if not result.wasSuccessful():
                failures = (result.errors + result.failures +
                            result.unexpectedSuccesses)
                if failures:
                    test, bt = failures[0]
                    name = test.test_data.get('name', test.id())
                    msg = 'From test "%s" :\n%s' % (name, bt)
                    self.fail(msg)

            self.assertTrue(result.wasSuccessful())


def test_maker(name, filename):
    def test(self):
        self._do_test(filename)
        test.__name__ = name
    return test


# Create one scenario per yaml file
for filename in os.listdir(TEST_DIR):
    if not filename.endswith('.yaml'):
        continue
    name = "test_%s" % filename[:-5].lower().replace("-", "_")
    setattr(GnocchiGabbiTest, name,
            test_maker(name, filename))
