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
from distutils import spawn
import math
import os
import sys

import daiquiri
from oslo_config import cfg
from oslo_policy import opts as policy_opts

from gnocchi import opts
from gnocchi.rest import app
from gnocchi import service
from gnocchi import utils


LOG = daiquiri.getLogger(__name__)


def prepare_service(conf=None):
    if conf is None:
        conf = cfg.ConfigOpts()

    opts.set_defaults()
    policy_opts.set_defaults(conf)
    conf = service.prepare_service(conf=conf)
    cfg_path = conf.oslo_policy.policy_file
    if not os.path.isabs(cfg_path):
        cfg_path = conf.find_file(cfg_path)
    if cfg_path is None or not os.path.exists(cfg_path):
        cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                   '..', 'rest', 'policy.json'))
    conf.set_default('policy_file', cfg_path, group='oslo_policy')
    return conf


def wsgi():
    return app.load_app(prepare_service())


def api():
    # Compat with previous pbr script
    try:
        double_dash = sys.argv.index("--")
    except ValueError:
        double_dash = None
    else:
        sys.argv.pop(double_dash)

    conf = cfg.ConfigOpts()
    for opt in opts.API_OPTS:
        # NOTE(jd) Register the API options without a default, so they are only
        # used to override the one in the config file
        c = copy.copy(opt)
        c.default = None
        conf.register_cli_opt(c)
    conf = prepare_service(conf)

    if double_dash is not None:
        # NOTE(jd) Wait to this stage to log so we're sure the logging system
        # is in place
        LOG.warning(
            "No need to pass `--' in gnocchi-api command line anymore, "
            "please remove")

    uwsgi = spawn.find_executable("uwsgi")
    if not uwsgi:
        LOG.error("Unable to find `uwsgi'.\n"
                  "Be sure it is installed and in $PATH.")
        return 1

    workers = utils.get_default_workers()

    # TODO(sileht): When uwsgi 2.1 will be release we should be able
    # to use --wsgi-manage-chunked-input
    # https://github.com/unbit/uwsgi/issues/1428
    args = [
        "--if-not-plugin", "python", "--plugin", "python", "--endif",
        "--%s" % conf.api.uwsgi_mode, "%s:%d" % (
            conf.host or conf.api.host,
            conf.port or conf.api.port),
        "--master",
        "--enable-threads",
        "--thunder-lock",
        "--hook-master-start", "unix_signal:15 gracefully_kill_them_all",
        "--die-on-term",
        "--processes", str(math.floor(workers * 1.5)),
        "--threads", str(workers),
        "--lazy-apps",
        "--chdir", "/",
        "--wsgi", "gnocchi.rest.wsgi",
        "--pyargv", " ".join(sys.argv[1:]),
    ]
    if conf.api.uwsgi_mode == "http":
        args.extend([
            "--so-keepalive",
            "--http-keepalive",
            "--add-header", "Connection: Keep-Alive"
        ])

    virtual_env = os.getenv("VIRTUAL_ENV")
    if virtual_env is not None:
        args.extend(["-H", os.getenv("VIRTUAL_ENV", ".")])

    return os.execl(uwsgi, uwsgi, *args)
