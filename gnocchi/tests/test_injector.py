# -*- encoding: utf-8 -*-
#
# Copyright © 2018 Red Hat, Inc.
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

from gnocchi.cli import injector
from gnocchi.tests import base


class InjectorTestCase(base.TestCase):
    def test_inject(self):
        self.assertEqual(100, injector._inject(
            self.incoming, self.coord, self.storage, self.index,
            measures=10, metrics=10))

    def test_inject_process(self):
        self.assertEqual(100, injector._inject(
            self.incoming, self.coord, self.storage, self.index,
            measures=10, metrics=10, process=True))
