# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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

import keystonemiddleware.auth_token
from oslo_config import cfg
from oslo_log import log
from oslo_policy import policy
from oslo_utils import importutils
from paste import deploy
import pecan
import webob.exc
from werkzeug import serving

from gnocchi import exceptions
from gnocchi import indexer
from gnocchi import json
from gnocchi import service
from gnocchi import storage


LOG = log.getLogger(__name__)


class GnocchiHook(pecan.hooks.PecanHook):

    def __init__(self, storage, indexer, conf):
        self.storage = storage
        self.indexer = indexer
        self.conf = conf
        self.policy_enforcer = policy.Enforcer(conf)

    def on_route(self, state):
        state.request.storage = self.storage
        state.request.indexer = self.indexer
        state.request.conf = self.conf
        state.request.policy_enforcer = self.policy_enforcer


class OsloJSONRenderer(object):
    @staticmethod
    def __init__(*args, **kwargs):
        pass

    @staticmethod
    def render(template_path, namespace):
        return json.dumps(namespace)


PECAN_CONFIG = {
    'app': {
        'root': 'gnocchi.rest.RootController',
        'modules': ['gnocchi.rest'],
    },
}


class NotImplementedMiddleware(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        try:
            return self.app(environ, start_response)
        except exceptions.NotImplementedError:
            raise webob.exc.HTTPNotImplemented(
                "Sorry, this Gnocchi server does "
                "not implement this feature 😞")


def load_app(conf, appname=None):
    # Build the WSGI app
    cfg_path = conf.api.paste_config
    if not os.path.isabs(cfg_path):
        cfg_path = conf.find_file(cfg_path)

    if cfg_path is None or not os.path.exists(cfg_path):
        raise cfg.ConfigFilesNotFoundError([conf.api.paste_config])

    LOG.info("WSGI config used: %s" % cfg_path)
    return deploy.loadapp("config:" + cfg_path, name=appname)


def setup_app(config=None, cfg=None):
    if cfg is None:
        # NOTE(jd) That sucks but pecan forces us to use kwargs :(
        raise RuntimeError("Config is actually mandatory")
    config = config or PECAN_CONFIG
    s = config.get('storage')
    if not s:
        s = storage.get_driver(cfg)
    i = config.get('indexer')
    if not i:
        i = indexer.get_driver(cfg)
        i.connect()

    # NOTE(sileht): pecan debug won't work in multi-process environment
    pecan_debug = cfg.api.pecan_debug
    if cfg.api.workers != 1 and pecan_debug:
        pecan_debug = False
        LOG.warning('pecan_debug cannot be enabled, if workers is > 1, '
                    'the value is overrided with False')

    app = pecan.make_app(
        config['app']['root'],
        debug=pecan_debug,
        hooks=(GnocchiHook(s, i, cfg),),
        guess_content_type_from_ext=False,
        custom_renderers={'json': OsloJSONRenderer},
    )

    if config.get('not_implemented_middleware', True):
        app = webob.exc.HTTPExceptionMiddleware(NotImplementedMiddleware(app))

    for middleware in reversed(cfg.api.middlewares):
        if not middleware:
            continue
        klass = importutils.import_class(middleware)
        # FIXME(jd) Find a way to remove that special handling…
        # next version of keystonemiddleware > 2.1.0 will support
        # 'oslo_config_project' option, so we could remove this
        # workaround.
        if klass == keystonemiddleware.auth_token.AuthProtocol:
            middleware_config = dict(cfg.keystone_authtoken)
        else:
            middleware_config = dict(cfg)
            # NOTE(sileht): Allow oslo.config compatible middleware to load
            # our configuration file.
            middleware_config['oslo_config_project'] = 'gnocchi'
        app = klass(app, middleware_config)

    return app


class WerkzeugApp(object):
    # NOTE(sileht): The purpose of this class is only to be used
    # with werkzeug to create the app after the werkzeug
    # fork gnocchi-api and avoid creation of connection of the
    # storage/indexer by the main process.

    def __init__(self, conf):
        self.app = None
        self.conf = conf

    def __call__(self, environ, start_response):
        if self.app is None:
            self.app = load_app(self.conf)
        return self.app(environ, start_response)


def build_server():
    conf = service.prepare_service()
    serving.run_simple(conf.api.host, conf.api.port,
                       WerkzeugApp(conf),
                       processes=conf.api.workers)


def app_factory(global_config, **local_conf):
    cfg = service.prepare_service()
    return setup_app(None, cfg=cfg)
