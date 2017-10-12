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

from gabbi import driver
# We need test_pytest so that pytest test collection works properly.
# Without this, the pytest_generate_tests method below will not be
# called.
from gabbi.driver import test_pytest  # noqa
import wsgi_intercept

from gnocchi.tests.functional import fixtures


wsgi_intercept.STRICT_RESPONSE_HEADERS = True
TESTS_DIR = 'gabbits'
PREFIX = '/gnocchi'


def pytest_generate_tests(metafunc):
    test_dir = os.path.join(os.path.dirname(__file__), TESTS_DIR)
    driver.py_test_generator(test_dir, prefix=PREFIX,
                             intercept=fixtures.setup_app,
                             fixture_module=fixtures, metafunc=metafunc,
                             safe_yaml=False)
