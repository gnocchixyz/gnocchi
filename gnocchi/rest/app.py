# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2016 eNovance
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
import pkg_resources
import uuid

import daiquiri
from oslo_middleware import cors
from oslo_policy import policy
from paste import deploy
import pecan
from pecan import jsonify
from stevedore import driver
import webob.exc

from gnocchi.cli import metricd
from gnocchi import exceptions
from gnocchi import incoming as gnocchi_incoming
from gnocchi import indexer as gnocchi_indexer
from gnocchi import json
from gnocchi import storage as gnocchi_storage


LOG = daiquiri.getLogger(__name__)


# Register our encoder by default for everything
jsonify.jsonify.register(object)(json.to_primitive)


class GnocchiHook(pecan.hooks.PecanHook):

    def __init__(self, storage, indexer, incoming, conf):
        self.storage = storage
        self.indexer = indexer
        self.incoming = incoming
        self.conf = conf
        self.policy_enforcer = policy.Enforcer(conf)
        self.auth_helper = driver.DriverManager("gnocchi.rest.auth_helper",
                                                conf.api.auth_mode,
                                                invoke_on_load=True).driver

    def on_route(self, state):
        state.request.storage = self.storage
        state.request.indexer = self.indexer
        state.request.incoming = self.incoming
        state.request.conf = self.conf
        state.request.policy_enforcer = self.policy_enforcer
        state.request.auth_helper = self.auth_helper


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

# NOTE(sileht): pastedeploy uses ConfigParser to handle
# global_conf, since python 3 ConfigParser doesn't
# allow to store object as config value, only strings are
# permit, so to be able to pass an object created before paste load
# the app, we store them into a global var. But the each loaded app
# store it's configuration in unique key to be concurrency safe.
global APPCONFIGS
APPCONFIGS = {}


def load_app(conf, indexer=None, storage=None, incoming=None, coord=None,
             not_implemented_middleware=True):
    global APPCONFIGS

    if not storage:
        if not coord:
            # NOTE(jd) This coordinator is never stop. I don't think it's a
            # real problem since the Web app can never really be stopped
            # anyway, except by quitting it entirely.
            coord = metricd.get_coordinator_and_start(conf.coordination_url)
        storage = gnocchi_storage.get_driver(conf, coord)
    if not incoming:
        incoming = gnocchi_incoming.get_driver(conf)
    if not indexer:
        indexer = gnocchi_indexer.get_driver(conf)

    # Build the WSGI app
    cfg_path = conf.api.paste_config
    if not os.path.isabs(cfg_path):
        cfg_path = conf.find_file(cfg_path)

    if cfg_path is None or not os.path.exists(cfg_path):
        LOG.debug("No api-paste configuration file found! Using default.")
        cfg_path = os.path.abspath(pkg_resources.resource_filename(
            __name__, "api-paste.ini"))

    config = dict(conf=conf, indexer=indexer, storage=storage,
                  incoming=incoming,
                  not_implemented_middleware=not_implemented_middleware)
    configkey = str(uuid.uuid4())
    APPCONFIGS[configkey] = config

    LOG.info("WSGI config used: %s", cfg_path)

    appname = "gnocchi+" + conf.api.auth_mode
    app = deploy.loadapp("config:" + cfg_path, name=appname,
                         global_conf={'configkey': configkey})
    return cors.CORS(app, conf=conf)


def _setup_app(root, conf, indexer, storage, incoming,
               not_implemented_middleware):
    app = pecan.make_app(
        root,
        hooks=(GnocchiHook(storage, indexer, incoming, conf),),
        guess_content_type_from_ext=False,
    )

    if not_implemented_middleware:
        app = webob.exc.HTTPExceptionMiddleware(NotImplementedMiddleware(app))

    return app


def app_factory(global_config, **local_conf):
    global APPCONFIGS
    appconfig = APPCONFIGS.get(global_config.get('configkey'))
    return _setup_app(root=local_conf.get('root'), **appconfig)
