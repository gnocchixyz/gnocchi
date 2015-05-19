#
# Copyright 2015 OpenStack Foundation
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

"""uuid_to_binary

Revision ID: f7d44b47928
Revises: 40c6aae14c3f
Create Date: 2015-04-30 13:29:29.074794

"""

# revision identifiers, used by Alembic.
revision = 'f7d44b47928'
down_revision = '40c6aae14c3f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy_utils.types.uuid


def upgrade():
    op.alter_column("metric", "id",
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                    nullable=False)

    for table in ('resource', 'resource_history', 'metric'):
        op.alter_column(table, "created_by_user_id",
                        type_=sqlalchemy_utils.types.uuid.UUIDType(
                            binary=True))
        op.alter_column(table, "created_by_project_id",
                        type_=sqlalchemy_utils.types.uuid.UUIDType(
                            binary=True))
    for table in ('resource', 'resource_history'):
        op.alter_column(table, "user_id",
                        type_=sqlalchemy_utils.types.uuid.UUIDType(
                            binary=True))
        op.alter_column(table, "project_id",
                        type_=sqlalchemy_utils.types.uuid.UUIDType(
                            binary=True))

    # Drop all foreign keys linking to resource.id
    for table in ('ceph_account', 'identity', 'volume', 'swift_account',
                  'ipmi', 'image', 'network', 'stack', 'instance',
                  'resource_history'):
        op.drop_constraint("fk_%s_id_resource_id" % table, table,
                           type_="foreignkey")

    op.drop_constraint("fk_metric_resource_id_resource_id", "metric",
                       type_="foreignkey")

    # Now change the type of resource.id
    op.alter_column("resource", "id",
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                    nullable=False)

    # Now change all the types of $table.id and re-add the FK
    for table in ('ceph_account', 'identity', 'volume', 'swift_account',
                  'ipmi', 'image', 'network', 'stack', 'instance',
                  'resource_history'):
        op.alter_column(
            table, "id",
            type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True),
            nullable=False)

        op.create_foreign_key("fk_%s_id_resource_id" % table,
                              table, "resource",
                              ("id",), ("id",),
                              ondelete="CASCADE")

    op.alter_column("metric", "resource_id",
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True))

    op.create_foreign_key("fk_metric_resource_id_resource_id",
                          "metric", "resource",
                          ("resource_id",), ("id",),
                          ondelete="CASCADE")
