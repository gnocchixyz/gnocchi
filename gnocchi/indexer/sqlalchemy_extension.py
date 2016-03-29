# -*- encoding: utf-8 -*-

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

import sqlalchemy
import sqlalchemy_utils

from gnocchi import resource_type


class Image(object):
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    container_format = sqlalchemy.Column(sqlalchemy.String(255),
                                         nullable=False)
    disk_format = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)


class Instance(object):
    flavor_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    image_ref = sqlalchemy.Column(sqlalchemy.String(255))
    host = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    server_group = sqlalchemy.Column(sqlalchemy.String(255))


class InstanceDisk(object):
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    instance_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                                    nullable=False)


class InstanceNetworkInterface(object):
    __tablename__ = 'instance_net_int'
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    instance_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                                    nullable=False)


class Volume(object):
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=True)


class Host(object):
    __tablename__ = 'host'
    host_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)


class HostNetworkInterface(object):
    __tablename__ = 'host_net_int'
    host_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    device_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=True)


class HostDisk(object):
    __tablename__ = 'host_disk'
    host_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    device_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=True)


class StringSchema(resource_type.StringSchema):
    @property
    def satype(self):
        return sqlalchemy.String(self.max_length)


class UUIDSchema(resource_type.UUIDSchema):
    satype = sqlalchemy_utils.UUIDType()
