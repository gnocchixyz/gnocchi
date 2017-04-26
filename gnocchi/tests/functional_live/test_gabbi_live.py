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
import six.moves.urllib.parse as urlparse


TESTS_DIR = 'gabbits'


def load_tests(loader, tests, pattern):
    """Provide a TestSuite to the discovery process."""
    gnocchi_url = os.getenv('GNOCCHI_ENDPOINT')
    if gnocchi_url:
        parsed_url = urlparse.urlsplit(gnocchi_url)
        prefix = parsed_url.path.rstrip('/')  # turn it into a prefix

        # NOTE(chdent): gabbi requires a port be passed or it will
        # default to 8001, so we must dance a little dance to get
        # the right ports. Probably gabbi needs to change.
        # https://github.com/cdent/gabbi/issues/50
        port = 443 if parsed_url.scheme == 'https' else 80
        if parsed_url.port:
            port = parsed_url.port

        test_dir = os.path.join(os.path.dirname(__file__), TESTS_DIR)
        return driver.build_tests(test_dir, loader,
                                  host=parsed_url.hostname,
                                  port=port,
                                  prefix=prefix)
    elif os.getenv("GABBI_LIVE"):
        raise RuntimeError('"GNOCCHI_ENDPOINT" is not set')
