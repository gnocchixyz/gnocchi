# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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

import socket
from wsgiref import simple_server

import netaddr
from oslo.config import cfg
from oslo.utils import importutils
from oslo_log import log
from oslo_serialization import jsonutils
import pecan

from gnocchi import indexer
from gnocchi import storage


LOG = log.getLogger(__name__)

cfg.CONF.import_opt('debug', 'gnocchi.openstack.common.log')

OPTS = [
    cfg.IntOpt('port',
               default=8041,
               help='The port for the Gnocchi API server.',
               ),
    cfg.StrOpt('host',
               default='0.0.0.0',
               help='The listen IP for the Gnocchi API server.',
               ),
    cfg.BoolOpt('pecan_debug',
                default='$debug',
                help='Toggle Pecan Debug Middleware. '
                'Defaults to global debug value.'
                ),
    cfg.MultiStrOpt('middlewares',
                    default=['keystonemiddleware.auth_token.AuthProtocol'],
                    help='Middlewares to use',),
]

opt_group = cfg.OptGroup(name='api',
                         title='Options for the gnocchi-api service')
cfg.CONF.register_group(opt_group)
cfg.CONF.register_opts(OPTS, opt_group)


class DBHook(pecan.hooks.PecanHook):

    def __init__(self, storage, indexer):
        self.storage = storage
        self.indexer = indexer

    def on_route(self, state):
        state.request.storage = self.storage
        state.request.indexer = self.indexer


class OsloJSONRenderer(object):
    @staticmethod
    def __init__(path, extra_vars):
        pass

    @staticmethod
    def render(template_path, namespace):
        return jsonutils.dumps(namespace)


PECAN_CONFIG = {
    'app': {
        'root': 'gnocchi.rest.RootController',
        'modules': ['gnocchi.rest'],
    },
    'conf': cfg.CONF,
}


def setup_app(pecan_config=PECAN_CONFIG):
    conf = pecan_config['conf']
    s = pecan_config.get('storage')
    if not s:
        s = storage.get_driver(conf)
    i = pecan_config.get('indexer')
    if not i:
        i = indexer.get_driver(conf)
    i.connect()

    app = pecan.make_app(
        pecan_config['app']['root'],
        debug=conf.api.pecan_debug,
        hooks=(DBHook(s, i),),
        guess_content_type_from_ext=False,
        custom_renderers={'json': OsloJSONRenderer},
    )

    for middleware in reversed(pecan_config['conf'].api.middlewares):
        if not middleware:
            continue
        klass = importutils.import_class(middleware)
        app = klass(app, dict(conf))

    return app


def get_server_cls(host):
    """Return an appropriate WSGI server class base on provided host

    :param host: The listen host for the ceilometer API server.
    """
    server_cls = simple_server.WSGIServer
    if netaddr.valid_ipv6(host):
        # NOTE(dzyu) make sure use IPv6 sockets if host is in IPv6 pattern
        if getattr(server_cls, 'address_family') == socket.AF_INET:
            class server_cls(server_cls):
                address_family = socket.AF_INET6
    return server_cls


def build_server():
    srv = simple_server.make_server(cfg.CONF.api.host,
                                    cfg.CONF.api.port,
                                    setup_app(),
                                    get_server_cls(cfg.CONF.api.host))
    srv.serve_forever()
