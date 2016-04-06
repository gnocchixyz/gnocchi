# Copyright 2016 OpenStack Foundation
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

"""create resource_type table

Revision ID: 828c16f70cce
Revises: 9901e5ea4b6e
Create Date: 2016-01-19 12:47:19.384127

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '828c16f70cce'
down_revision = '9901e5ea4b6e'
branch_labels = None
depends_on = None


type_string = sa.String(255)
type_enum = sa.Enum('generic', 'instance',
                    'swift_account', 'volume',
                    'ceph_account', 'network',
                    'identity', 'ipmi', 'stack',
                    'image', 'instance_disk',
                    'instance_network_interface',
                    'host', 'host_disk',
                    'host_network_interface',
                    name="resource_type_enum")


def type_string_col(name, table):
    return sa.Column(
        name, type_string,
        sa.ForeignKey('resource_type.name',
                      ondelete="RESTRICT",
                      name="fk_%s_resource_type_name" % table))


def type_enum_col(name):
    return sa.Column(name, type_enum,
                     nullable=False, default='generic')


def upgrade():
    resource_type = op.create_table(
        'resource_type',
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint('name'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )

    resource = sa.Table('resource', sa.MetaData(),
                        type_string_col("type", "resource"))
    op.execute(resource_type.insert().from_select(
        ['name'], sa.select([resource.c.type]).distinct()))

    for table in ["resource", "resource_history"]:
        op.alter_column(table, "type", new_column_name="old_type",
                        existing_type=type_enum)
        op.add_column(table, type_string_col("type", table))
        sa_table = sa.Table(table, sa.MetaData(),
                            type_string_col("type", table),
                            type_enum_col('old_type'))
        op.execute(sa_table.update().values(
            {sa_table.c.type: sa_table.c.old_type}))
        op.drop_column(table, "old_type")
        op.alter_column(table, "type", nullable=False,
                        existing_type=type_string)
