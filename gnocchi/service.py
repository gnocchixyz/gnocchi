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
import os

from oslo_config import cfg
from oslo_db import options as db_options
from oslo_log import log
from oslo_policy import opts as policy_opts
import pbr.version
from six.moves.urllib import parse as urlparse

from gnocchi import archive_policy
from gnocchi import opts
from gnocchi import utils

LOG = log.getLogger(__name__)


def prepare_service(args=None, conf=None,
                    default_config_files=None):
    if conf is None:
        conf = cfg.ConfigOpts()
    opts.set_defaults()
    # FIXME(jd) Use the pkg_entry info to register the options of these libs
    log.register_options(conf)
    db_options.set_defaults(conf)
    policy_opts.set_defaults(conf, policy_file=os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "rest", "policy.json")))

    # Register our own Gnocchi options
    for group, options in opts.list_opts():
        conf.register_opts(list(options),
                           group=None if group == "DEFAULT" else group)

    conf.set_default("workers", utils.get_default_workers(), group="metricd")

    version = pbr.version.VersionInfo('gnocchi').version_string()
    conf(args, project='gnocchi', validate_default_values=True,
         default_config_files=default_config_files,
         version=version)

    # HACK(jd) I'm not happy about that, fix AP class to handle a conf object?
    archive_policy.ArchivePolicy.DEFAULT_AGGREGATION_METHODS = (
        conf.archive_policy.default_aggregation_methods
    )

    # If no coordination URL is provided, default to using the indexer as
    # coordinator
    if conf.storage.coordination_url is None:
        parsed = urlparse.urlparse(conf.indexer.url)
        proto, _, _ = parsed.scheme.partition("+")
        parsed = list(parsed)
        # Set proto without the + part
        parsed[0] = proto
        conf.set_default("coordination_url",
                         urlparse.urlunparse(parsed),
                         "storage")

    log.set_defaults(default_log_levels=log.get_default_log_levels() +
                     ["passlib.utils.compat=INFO", "swiftclient=WARNING"])
    log.setup(conf, 'gnocchi')
    LOG.info("Gnocchi version %s", version)
    conf.log_opt_values(LOG, log.DEBUG)

    return conf
