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
from six.moves.urllib.parse import quote

try:
    from swiftclient import client as swclient
    from swiftclient import utils as swift_utils
except ImportError:
    swclient = None
    swift_utils = None

from gnocchi import storage

LOG = log.getLogger(__name__)


def get_connection(conf):
    if swclient is None:
        raise RuntimeError("python-swiftclient unavailable")

    return swclient.Connection(
        auth_version=conf.swift_auth_version,
        authurl=conf.swift_authurl,
        preauthtoken=conf.swift_preauthtoken,
        user=conf.swift_user,
        key=conf.swift_key,
        tenant_name=conf.swift_project_name,
        timeout=conf.swift_timeout,
        os_options={'endpoint_type': conf.swift_endpoint_type,
                    'user_domain_name': conf.swift_user_domain_name},
        retries=0)


POST_HEADERS = {'Accept': 'application/json', 'Content-Type': 'text/plain'}


def bulk_delete(conn, container, objects):
    objects = [quote(('/%s/%s' % (container, obj['name'])).encode('utf-8'))
               for obj in objects]
    resp = {}
    headers, body = conn.post_account(
        headers=POST_HEADERS, query_string='bulk-delete',
        data=b''.join(obj.encode('utf-8') + b'\n' for obj in objects),
        response_dict=resp)
    if resp['status'] != 200:
        raise storage.StorageError(
            "Unable to bulk-delete, is bulk-delete enabled in Swift?")
    resp = swift_utils.parse_api_response(headers, body)
    LOG.debug('# of objects deleted: %s, # of objects skipped: %s',
              resp['Number Deleted'], resp['Number Not Found'])
