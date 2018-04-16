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

import daiquiri
from oslo_config import cfg
from oslo_db import options as db_options
from six.moves.urllib import parse as urlparse

import gnocchi
from gnocchi import archive_policy
from gnocchi import opts
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


def prepare_service(args=None, conf=None,
                    default_config_files=None,
                    log_to_std=False, logging_level=None,
                    skip_log_opts=False):
    if conf is None:
        conf = cfg.ConfigOpts()
    # FIXME(jd) Use the pkg_entry info to register the options of these libs
    db_options.set_defaults(conf)

    # Register our own Gnocchi options
    for group, options in opts.list_opts():
        conf.register_opts(list(options),
                           group=None if group == "DEFAULT" else group)

    conf.register_cli_opts(opts._cli_options)

    workers = utils.get_default_workers()
    conf.set_default("workers", workers, group="metricd")
    conf.set_default("parallel_operations", workers)

    conf(args, project='gnocchi', validate_default_values=True,
         default_config_files=default_config_files,
         version=gnocchi.__version__)

    utils.parallel_map.MAX_WORKERS = conf.parallel_operations

    if not log_to_std and (conf.log_dir or conf.log_file):
        outputs = [daiquiri.output.File(filename=conf.log_file,
                                        directory=conf.log_dir)]
    else:
        outputs = [daiquiri.output.STDERR]

    if conf.use_syslog:
        outputs.append(
            daiquiri.output.Syslog(facility=conf.syslog_log_facility))

    if conf.use_journal:
        outputs.append(daiquiri.output.Journal())

    daiquiri.setup(outputs=outputs)
    if logging_level is None:
        if conf.debug:
            logging_level = logging.DEBUG
        elif conf.verbose:
            logging_level = logging.INFO
        else:
            logging_level = logging.WARNING
    logging.getLogger("gnocchi").setLevel(logging_level)

    # HACK(jd) I'm not happy about that, fix AP class to handle a conf object?
    archive_policy.ArchivePolicy.DEFAULT_AGGREGATION_METHODS = (
        conf.archive_policy.default_aggregation_methods
    )

    # If no coordination URL is provided, default to using the indexer as
    # coordinator
    if conf.coordination_url is None:
        if conf.storage.driver == "redis":
            conf.set_default("coordination_url",
                             conf.storage.redis_url)
        elif conf.incoming.driver == "redis":
            conf.set_default("coordination_url",
                             conf.incoming.redis_url)
        else:
            parsed = urlparse.urlparse(conf.indexer.url)
            proto, _, _ = parsed.scheme.partition("+")
            parsed = list(parsed)
            # Set proto without the + part
            parsed[0] = proto
            conf.set_default("coordination_url",
                             urlparse.urlunparse(parsed))

    if not skip_log_opts:
        LOG.info("Gnocchi version %s", gnocchi.__version__)
        conf.log_opt_values(LOG, logging.DEBUG)

    return conf
