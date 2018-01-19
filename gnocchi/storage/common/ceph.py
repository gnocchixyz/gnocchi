# -*- encoding: utf-8 -*-
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

from oslo_log import log
from oslo_utils import importutils

LOG = log.getLogger(__name__)


for RADOS_MODULE_NAME in ('cradox', 'rados'):
    rados = importutils.try_import(RADOS_MODULE_NAME)
    if rados is not None:
        break
else:
    RADOS_MODULE_NAME = None

if rados is not None and hasattr(rados, 'run_in_thread'):
    rados.run_in_thread = lambda target, args, timeout=None: target(*args)
    LOG.info("rados.run_in_thread is monkeypatched.")


def create_rados_connection(conf):
    options = {}
    if conf.ceph_keyring:
        options['keyring'] = conf.ceph_keyring
    if conf.ceph_secret:
        options['key'] = conf.ceph_secret
    if conf.ceph_timeout:
        options['rados_osd_op_timeout'] = conf.ceph_timeout
        options['rados_mon_op_timeout'] = conf.ceph_timeout
        options['client_mount_timeout'] = conf.ceph_timeout

    if not rados:
        raise ImportError("No module named 'rados' nor 'cradox'")

    if not hasattr(rados, 'OmapIterator'):
        raise ImportError("Your rados python module does not support "
                          "omap feature. Install 'cradox' (recommended) "
                          "or upgrade 'python-rados' >= 9.1.0 ")

    LOG.info("Ceph storage backend use '%s' python library",
             RADOS_MODULE_NAME)

    # NOTE(sileht): librados handles reconnection itself,
    # by default if a call timeout (30sec), it raises
    # a rados.Timeout exception, and librados
    # still continues to reconnect on the next call
    conn = rados.Rados(conffile=conf.ceph_conffile,
                       rados_id=conf.ceph_username,
                       conf=options)
    try:
        conn.connect()
    except rados.InvalidArgumentError:
        raise Exception("Unable to connect to ceph, check the configuration")
    ioctx = conn.open_ioctx(conf.ceph_pool)
    return conn, ioctx


def close_rados_connection(conn, ioctx):
    ioctx.aio_flush()
    ioctx.close()
    conn.shutdown()
