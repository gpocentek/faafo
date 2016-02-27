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

import copy

from oslo_config import cfg
from oslo_log import log
import swiftclient.client

LOG = log.getLogger('faafo.swift')
CONF = cfg.CONF

swift_opts = [
    cfg.StrOpt('auth-url',
               default='http://127.0.0.1:5000/v2.0',
               help='Swift authentication URL'),
    cfg.StrOpt('username',
               default='',
               help='Swift login'),
    cfg.StrOpt('password',
               default='',
               help='Swift password'),
    cfg.StrOpt('tenant-name',
               default='',
               help='Swift tenant name'),
    cfg.StrOpt('auth-version',
               default='2.0',
               help='Swift authentication version'),
    cfg.StrOpt('container-name',
               default='faafo',
               help='Container name')
]

CONF.register_opts(swift_opts, 'swift')


def list_opts():
    """Entry point for oslo-config-generator."""
    return [('swift', copy.deepcopy(swift_opts))]


class SwiftClient(object):
    def __init__(self):
        self._conn = swiftclient.client.Connection(
            authurl=CONF.swift.auth_url,
            user=CONF.swift.username,
            key=CONF.swift.password,
            tenant_name=CONF.swift.tenant_name,
            auth_version=CONF.swift.auth_version)
        self._conn.get_account()
        self._url = self._conn.url
        self._container = CONF.swift.container_name
        LOG.info("container: %s" % self._container)
        LOG.info("url: %s" % self._url)
        headers = {'X-Container-Read': '.r:*'}
        self._conn.put_container(self._container, headers)

    @property
    def container_url(self):
        return "%s/%s" % (self._url, self._container)

    def object_url(self, uuid):
        return "%s/%s" % (self.container_url, uuid)

    def write_image(self, uuid, content):
        self._conn.put_object(self._container, uuid, contents=content,
                               content_type='image/png')
        return self.object_url(uuid)

    def delete_image(self, uuid):
        self._conn.delete_object(self._container, uuid)
