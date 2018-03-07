# Copyright (c) 2013 Mirantis Inc.
# Copyright (c) 2015-2017 Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import copy
import os
import sys

import daiquiri
from oslo_config import cfg
from oslo_config import generator
import six

from gnocchi import archive_policy
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage


LOG = daiquiri.getLogger(__name__)


def config_generator():
    args = sys.argv[1:]
    if args is None:
        args = ['--output-file', 'etc/gnocchi/gnocchi.conf']
    return generator.main(['--config-file',
                           '%s/../gnocchi-config-generator.conf' %
                           os.path.dirname(__file__)]
                          + args)


_SACK_NUMBER_OPT = cfg.IntOpt(
    "sacks-number", min=1, max=65535, required=True,
    help="Number of incoming storage sacks to create.")


def upgrade():
    conf = cfg.ConfigOpts()
    sack_number_opt = copy.copy(_SACK_NUMBER_OPT)
    sack_number_opt.default = 128
    conf.register_cli_opts([
        cfg.BoolOpt("skip-index", default=False,
                    help="Skip index upgrade."),
        cfg.BoolOpt("skip-storage", default=False,
                    help="Skip storage upgrade."),
        cfg.BoolOpt("skip-incoming", default=False,
                    help="Skip incoming storage upgrade."),
        cfg.BoolOpt("skip-archive-policies-creation", default=False,
                    help="Skip default archive policies creation."),
        sack_number_opt,
    ])
    conf = service.prepare_service(conf=conf, log_to_std=True)
    if not conf.skip_index:
        index = indexer.get_driver(conf)
        LOG.info("Upgrading indexer %s", index)
        index.upgrade()
    if not conf.skip_storage:
        s = storage.get_driver(conf)
        LOG.info("Upgrading storage %s", s)
        s.upgrade()
    if not conf.skip_incoming:
        i = incoming.get_driver(conf)
        LOG.info("Upgrading incoming storage %s", i)
        i.upgrade(conf.sacks_number)

    if (not conf.skip_archive_policies_creation
            and not index.list_archive_policies()
            and not index.list_archive_policy_rules()):
        if conf.skip_index:
            index = indexer.get_driver(conf)
        for name, ap in six.iteritems(archive_policy.DEFAULT_ARCHIVE_POLICIES):
            index.create_archive_policy(ap)
        index.create_archive_policy_rule("default", "*", "low")


def change_sack_size():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([_SACK_NUMBER_OPT])
    conf = service.prepare_service(conf=conf, log_to_std=True)
    s = incoming.get_driver(conf)
    try:
        report = s.measures_report(details=False)
    except incoming.SackDetectionError:
        LOG.error('Unable to detect the number of storage sacks.\n'
                  'Ensure gnocchi-upgrade has been executed.')
        return
    remainder = report['summary']['measures']
    if remainder:
        LOG.error('Cannot change sack when non-empty backlog. Process '
                  'remaining %s measures and try again', remainder)
        return
    LOG.info("Removing current %d sacks", s.NUM_SACKS)
    s.remove_sacks()
    LOG.info("Creating new %d sacks", conf.sacks_number)
    s.upgrade(conf.sacks_number)
