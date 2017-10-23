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

"""A test module to exercise the Gnocchi API with gabbi."""

import os
import unittest

from gabbi import driver
import wsgi_intercept

from gnocchi.tests import base
from gnocchi.tests.functional import fixtures


wsgi_intercept.STRICT_RESPONSE_HEADERS = True
TESTS_DIR = 'gabbits'
PREFIX = '/gnocchi'


class TestFunctional(base.BaseTestCase):
    # NOTE(sileht): This run each yaml file into one testcase
    # instead of one testcase per yaml line
    # This permits to use pytest-xdist any --dist

    def _do_test(self, test):
        with open(os.devnull, 'w') as stream:
            result = unittest.TextTestRunner(
                stream=stream, verbosity=0, failfast=True
            ).run(test)

        if not result.wasSuccessful():
            failures = (result.errors + result.failures +
                        result.unexpectedSuccesses)
            if failures:
                test, bt = failures[0]
                name = test.test_data.get('name', test.id())
                msg = 'From test "%s" :\n%s' % (name, bt)
                self.fail(msg)

        self.assertTrue(result.wasSuccessful())

    @staticmethod
    def _test_maker(name, t):
        def test(self):
            self._do_test(t)
            test.__name__ = name
        return test

    @classmethod
    def generate_tests(cls):
        test_dir = os.path.join(os.path.dirname(__file__), TESTS_DIR)
        loader = unittest.TestLoader()
        tests = driver.build_tests(test_dir, loader,
                                   intercept=fixtures.setup_app,
                                   fixture_module=fixtures,
                                   prefix=PREFIX, safe_yaml=False)
        for test in tests:
            name = test._tests[0].__class__.__name__
            setattr(cls, name, cls._test_maker(name, test))


TestFunctional.generate_tests()
