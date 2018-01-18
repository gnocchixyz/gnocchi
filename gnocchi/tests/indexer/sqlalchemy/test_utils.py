# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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
from gnocchi import indexer
from gnocchi.tests import base


class TestUtils(base.TestCase):
    def test_percent_in_url(self):
        url = 'mysql+pymysql://user:pass%word@localhost/foobar'
        self.conf.set_override('url', url, 'indexer')
        alembic = indexer.get_driver(self.conf)._get_alembic_config()
        self.assertEqual(url, alembic.get_main_option("sqlalchemy.url"))
