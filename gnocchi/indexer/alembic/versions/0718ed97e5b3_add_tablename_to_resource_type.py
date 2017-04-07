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

"""Add tablename to resource_type

Revision ID: 0718ed97e5b3
Revises: 828c16f70cce
Create Date: 2016-01-20 08:14:04.893783

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0718ed97e5b3'
down_revision = '828c16f70cce'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("resource_type", sa.Column('tablename', sa.String(18),
                                             nullable=True))

    resource_type = sa.Table(
        'resource_type', sa.MetaData(),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('tablename', sa.String(18), nullable=True)
    )
    op.execute(resource_type.update().where(
        resource_type.c.name == "instance_network_interface"
    ).values({'tablename': op.inline_literal("'instance_net_int'")}))
    op.execute(resource_type.update().where(
        resource_type.c.name != "instance_network_interface"
    ).values({'tablename': resource_type.c.name}))

    op.alter_column("resource_type", "tablename", type_=sa.String(18),
                    nullable=False)
    op.create_unique_constraint("uniq_resource_type0tablename",
                                "resource_type", ["tablename"])
