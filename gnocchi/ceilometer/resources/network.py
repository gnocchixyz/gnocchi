#
# Copyright 2015 Mirantis Inc.
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
import sqlalchemy
import sqlalchemy_utils

from gnocchi.ceilometer.resources import base
from gnocchi.indexer import sqlalchemy_base


class Network(base.ResourceBase):
    @staticmethod
    def get_resource_extra_attributes(sample):
        return {}

    @staticmethod
    def get_metrics_names():
        return ['bandwidth',
                'network',
                'network.create',
                'network.update',
                'subnet',
                'subnet.create',
                'subnet.update',
                'port',
                'port.create',
                'port.update',
                'router',
                'router.create',
                'router.update',
                'ip.floating',
                'ip.floating.create',
                'ip.floating.update',
                ]


class NetworkSQLAlchemy(sqlalchemy_base.Resource):
    __tablename__ = 'network'
    __table_args__ = (
        sqlalchemy.Index('ix_network_id', 'id'),
        sqlalchemy_base.COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)
