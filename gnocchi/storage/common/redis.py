# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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

from __future__ import absolute_import

from six.moves.urllib import parse

try:
    import redis
    from redis import sentinel
except ImportError:
    redis = None
    sentinel = None

from gnocchi import utils


SEP = ':'

CLIENT_ARGS = frozenset([
    'db',
    'encoding',
    'retry_on_timeout',
    'socket_keepalive',
    'socket_timeout',
    'ssl',
    'ssl_certfile',
    'ssl_keyfile',
    'sentinel',
    'sentinel_fallback',
])
"""
Keys that we allow to proxy from the coordinator configuration into the
redis client (used to configure the redis client internals so that
it works as you expect/want it to).

See: http://redis-py.readthedocs.org/en/latest/#redis.Redis

See: https://github.com/andymccurdy/redis-py/blob/2.10.3/redis/client.py
"""

#: Client arguments that are expected/allowed to be lists.
CLIENT_LIST_ARGS = frozenset([
    'sentinel_fallback',
])

#: Client arguments that are expected to be boolean convertible.
CLIENT_BOOL_ARGS = frozenset([
    'retry_on_timeout',
    'ssl',
])

#: Client arguments that are expected to be int convertible.
CLIENT_INT_ARGS = frozenset([
    'db',
    'socket_keepalive',
    'socket_timeout',
])

#: Default socket timeout to use when none is provided.
CLIENT_DEFAULT_SOCKET_TO = 30


def get_client(conf):
    if redis is None:
        raise RuntimeError("python-redis unavailable")
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
    if parsed_url.password:
        kwargs['password'] = parsed_url.password

    for a in CLIENT_ARGS:
        if a not in options:
            continue
        if a in CLIENT_BOOL_ARGS:
            v = utils.strtobool(options[a][-1])
        elif a in CLIENT_LIST_ARGS:
            v = options[a]
        elif a in CLIENT_INT_ARGS:
            v = int(options[a][-1])
        else:
            v = options[a][-1]
        kwargs[a] = v
    if 'socket_timeout' not in kwargs:
        kwargs['socket_timeout'] = CLIENT_DEFAULT_SOCKET_TO

    # Ask the sentinel for the current master if there is a
    # sentinel arg.
    if 'sentinel' in kwargs:
        sentinel_hosts = [
            tuple(fallback.split(':'))
            for fallback in kwargs.get('sentinel_fallback', [])
        ]
        sentinel_hosts.insert(0, (kwargs['host'], kwargs['port']))
        sentinel_server = sentinel.Sentinel(
            sentinel_hosts,
            socket_timeout=kwargs['socket_timeout'])
        sentinel_name = kwargs['sentinel']
        del kwargs['sentinel']
        if 'sentinel_fallback' in kwargs:
            del kwargs['sentinel_fallback']
        master_client = sentinel_server.master_for(sentinel_name, **kwargs)
        # The master_client is a redis.StrictRedis using a
        # Sentinel managed connection pool.
        return master_client
    return redis.StrictRedis(**kwargs)
