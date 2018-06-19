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
from oslo_config import cfg
from oslo_policy import opts as policy_opts
<<<<<<< HEAD
<<<<<<< HEAD
import six
=======
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels

from gnocchi import opts


<<<<<<< HEAD
<<<<<<< HEAD
def list_all_incoming_metrics(incoming):
    return set.union(*[incoming.list_metric_with_measures_to_process(i)
                       for i in six.moves.range(incoming.NUM_SACKS)])


=======
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
def prepare_conf():
    conf = cfg.ConfigOpts()

    opts.set_defaults()
    policy_opts.set_defaults(conf)
    return conf
