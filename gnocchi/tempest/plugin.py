# -*- encoding: utf-8 -*-
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

from __future__ import absolute_import

import os

from tempest.test_discover import plugins

import gnocchi
from gnocchi.tempest import config as tempest_config


class GnocchiTempestPlugin(plugins.TempestPlugin):
    def load_tests(self):
        base_path = os.path.split(os.path.dirname(
            os.path.abspath(gnocchi.__file__)))[0]
        test_dir = "gnocchi/tempest"
        full_test_dir = os.path.join(base_path, test_dir)
        return full_test_dir, base_path

    def register_opts(self, conf):
        conf.register_opt(tempest_config.service_option,
                          group='service_available')
        conf.register_group(tempest_config.metric_group)
        conf.register_opts(tempest_config.metric_opts, group='metric')

    def get_opt_lists(self):
        return [(tempest_config.metric_group.name,
                 tempest_config.metric_opts),
                ('service_available', [tempest_config.service_option])]
