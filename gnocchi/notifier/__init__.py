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
import os

from oslo_config import cfg
from six.moves.urllib import parse
from stevedore import driver

from gnocchi import exceptions

OPTS = [
    cfg.StrOpt('url',
               secret=True,
               default=os.getenv("GNOCCHI_NOTIFIER_URL"),
               help='URL of the notifier to use'),
]


def get_driver(conf):
    """Return the configured driver."""
    if conf.notifier.url:
        split = parse.urlsplit(conf.notifier.url)
        d = driver.DriverManager('gnocchi.notifier', split.scheme).driver
        return d(conf.notifier)
    return Notifier(conf.notifier)


class Notifier(object):
    @staticmethod
    def __init__(conf):
        pass

    @staticmethod
    def notify_new_measures_for_sacks(sacks):
        pass

    @staticmethod
    def iter_on_sacks_to_process():
        """Return an iterable of sack that got new things to process."""
        raise exceptions.NotImplementedError
