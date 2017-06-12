# Copyright (c) 2016-2017 Red Hat, Inc.
# Copyright (c) 2015 eNovance
# Copyright (c) 2013 Mirantis Inc.
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
import logging
import os

import daiquiri
from oslo_config import cfg
from oslo_db import options as db_options
from oslo_policy import opts as policy_opts
import pbr.version
from six.moves.urllib import parse as urlparse

from gnocchi import archive_policy
from gnocchi import opts
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


def prepare_service(args=None, conf=None,
                    default_config_files=None):
    if conf is None:
        conf = cfg.ConfigOpts()
    opts.set_defaults()
    # FIXME(jd) Use the pkg_entry info to register the options of these libs
    db_options.set_defaults(conf)
    policy_opts.set_defaults(conf)

    # Register our own Gnocchi options
    for group, options in opts.list_opts():
        conf.register_opts(list(options),
                           group=None if group == "DEFAULT" else group)

    conf.register_cli_opts(opts._cli_options)

    conf.set_default("workers", utils.get_default_workers(), group="metricd")

    conf(args, project='gnocchi', validate_default_values=True,
         default_config_files=default_config_files,
         version=pbr.version.VersionInfo('gnocchi').version_string())

    if conf.log_dir or conf.log_file:
        outputs = [daiquiri.output.File(filename=conf.log_file,
                                        directory=conf.log_dir)]
    else:
        outputs = [daiquiri.output.STDERR]

    if conf.use_syslog:
        outputs.append(
            daiquiri.output.Syslog(facilty=conf.syslog_log_faciltity))

    if conf.use_journal:
        outputs.append(daiquiri.output.Journal())

    daiquiri.setup(outputs=outputs)
    if conf.debug:
        level = logging.DEBUG
    elif conf.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.getLogger("gnocchi").setLevel(level)

    # HACK(jd) I'm not happy about that, fix AP class to handle a conf object?
    archive_policy.ArchivePolicy.DEFAULT_AGGREGATION_METHODS = (
        conf.archive_policy.default_aggregation_methods
    )

    # If no coordination URL is provided, default to using the indexer as
    # coordinator
    if conf.storage.coordination_url is None:
        if conf.storage.driver == "redis":
            conf.set_default("coordination_url",
                             conf.storage.redis_url,
                             "storage")
        elif conf.incoming.driver == "redis":
            conf.set_default("coordination_url",
                             conf.incoming.redis_url,
                             "storage")
        else:
            parsed = urlparse.urlparse(conf.indexer.url)
            proto, _, _ = parsed.scheme.partition("+")
            parsed = list(parsed)
            # Set proto without the + part
            parsed[0] = proto
            conf.set_default("coordination_url",
                             urlparse.urlunparse(parsed),
                             "storage")

    cfg_path = conf.oslo_policy.policy_file
    if not os.path.isabs(cfg_path):
        cfg_path = conf.find_file(cfg_path)
    if cfg_path is None or not os.path.exists(cfg_path):
        cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                'rest', 'policy.json'))
    conf.set_default('policy_file', cfg_path, group='oslo_policy')

    conf.log_opt_values(LOG, logging.DEBUG)

    return conf
