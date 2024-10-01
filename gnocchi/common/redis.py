# -*- encoding: utf-8 -*-
#
# Copyright © 2017-2018 Red Hat
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
from oslo_utils import netutils
from oslo_utils import strutils
from urllib import parse

try:
    import redis
    from redis import sentinel
except ImportError:
    redis = None
    sentinel = None


SEP_S = ':'
SEP = b':'

CLIENT_ARGS = frozenset([
    'db',
    'encoding',
    'health_check_interval',
    'retry_on_timeout',
    'socket_keepalive',
    'socket_timeout',
    'ssl',
    'ssl_certfile',
    'ssl_keyfile',
    'ssl_ca_certs',
    'sentinel',
    'sentinel_fallback',
    'sentinel_username',
    'sentinel_password',
    'sentinel_ssl'
])
"""
"""

#: Client arguments that are expected/allowed to be lists.
CLIENT_LIST_ARGS = frozenset([
    'sentinel_fallback',
])

#: Client arguments that are expected to be boolean convertible.
CLIENT_BOOL_ARGS = frozenset([
    'retry_on_timeout',
    'socket_keepalive',
    'ssl',
    'sentinel_ssl'
])

#: Client arguments that are expected to be int convertible.
CLIENT_INT_ARGS = frozenset([
    'db',
])

#: Client arguments that are expected to be float convertible.
CLIENT_FLOAT_ARGS = frozenset([
    'socket_timeout',
    'health_check_interval',
])

OPTS = [
    cfg.StrOpt('redis_url',
               secret=True,
               default='redis://localhost:6379/',
               help="""Redis URL

  For example::

    redis://[username:password]@localhost:6379?db=0

  We proxy some options to the redis client (used to configure the redis client
  internals so that it works as you expect/want it to):  `%s`

  Further resources/links:

   - http://redis-py.readthedocs.org/en/latest/#redis.Redis
   - https://github.com/andymccurdy/redis-py/blob/2.10.3/redis/client.py

  To use a `sentinel`_ the connection URI must point to the sentinel server.
  At connection time the sentinel will be asked for the current IP and port
  of the master and then connect there. The connection URI for sentinel
  should be written as follows::

    redis://<sentinel host>:<sentinel port>?sentinel=<master name>

  Additional sentinel hosts are listed with multiple ``sentinel_fallback``
  parameters as follows::

      redis://<sentinel host>:<sentinel port>?sentinel=<master name>&
        sentinel_fallback=<other sentinel host>:<sentinel port>&
        sentinel_fallback=<other sentinel host>:<sentinel port>&
        sentinel_fallback=<other sentinel host>:<sentinel port>

  Further resources/links:

  - http://redis.io/
  - http://redis.io/topics/sentinel
  - http://redis.io/topics/cluster-spec

""" % "`, `".join(sorted(CLIENT_ARGS))),
]


def _parse_sentinel(sentinel):
    host, port = netutils.parse_host_port(sentinel)
    if host is None or port is None:
        raise ValueError('Malformed sentinel server format')
    return (host, port)


def get_client(conf, scripts=None):
    if redis is None:
        raise RuntimeError("Redis Python module is unavailable")
    parsed_url = parse.urlparse(conf.redis_url)
    options = parse.parse_qs(parsed_url.query)

    kwargs = {}
    if parsed_url.hostname:
        kwargs['host'] = parsed_url.hostname
        if parsed_url.port:
            kwargs['port'] = parsed_url.port
    else:
        if not parsed_url.path:
            raise ValueError("Expected socket path in parsed urls path")
        kwargs['unix_socket_path'] = parsed_url.path
    if parsed_url.username:
        kwargs['username'] = parsed_url.username
    if parsed_url.password:
        kwargs['password'] = parsed_url.password

    for a in CLIENT_ARGS:
        if a not in options:
            continue
        if a in CLIENT_BOOL_ARGS:
            v = strutils.bool_from_string(options[a][-1])
        elif a in CLIENT_LIST_ARGS:
            v = options[a]
        elif a in CLIENT_INT_ARGS:
            v = int(options[a][-1])
        elif a in CLIENT_FLOAT_ARGS:
            v = float(options[a][-1])
        else:
            v = options[a][-1]
        kwargs[a] = v

    # Ask the sentinel for the current master if there is a
    # sentinel arg.
    if 'sentinel' in kwargs:
        sentinel_hosts = [
            _parse_sentinel(fallback)
            for fallback in kwargs.pop('sentinel_fallback', [])
        ]
        sentinel_hosts.insert(0, (kwargs.pop('host'), kwargs.pop('port')))
        sentinel_name = kwargs.pop('sentinel')
        sentinel_kwargs = {}
        # NOTE(tkajinam): Copy socket_* options, according to the logic
        # in redis-py
        for key in kwargs:
            if key.startswith('socket_'):
                sentinel_kwargs[key] = kwargs[key]
        if kwargs.pop('sentinel_ssl', False):
            sentinel_kwargs['ssl'] = True
            for key in ('ssl_certfile', 'ssl_keyfile', 'ssl_cafile'):
                if key in kwargs:
                    sentinel_kwargs[key] = kwargs[key]
        for key in ('username', 'password'):
            if 'sentinel_' + key in kwargs:
                sentinel_kwargs[key] = kwargs.pop('sentinel_' + key)
        sentinel_server = sentinel.Sentinel(
            sentinel_hosts,
            sentinel_kwargs=sentinel_kwargs,
            **kwargs)
        # The client is a redis.StrictRedis using a
        # Sentinel managed connection pool.
        client = sentinel_server.master_for(sentinel_name)
    else:
        client = redis.StrictRedis(**kwargs)

    if scripts is not None:
        scripts = {
            name: client.register_script(code)
            for name, code in scripts.items()
        }

    return client, scripts
