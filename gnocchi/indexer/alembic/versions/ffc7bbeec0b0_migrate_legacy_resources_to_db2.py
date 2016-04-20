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

"""migrate_legacy_resources_to_db2

Revision ID: ffc7bbeec0b0
Revises: 8f376189b9eb
Create Date: 2016-04-14 15:57:13.072128

"""
import json

from alembic import op
import sqlalchemy as sa

from gnocchi.indexer import sqlalchemy_legacy_resources as legacy

# revision identifiers, used by Alembic.
revision = 'ffc7bbeec0b0'
down_revision = '8f376189b9eb'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()

    resource_type = sa.Table(
        'resource_type', sa.MetaData(),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('tablename', sa.String(18), nullable=False),
        sa.Column('attributes', sa.Text, nullable=False)
    )

    # NOTE(gordc): fix for incorrect migration:
    # 0735ed97e5b3_add_tablename_to_resource_type.py#L46
    op.execute(resource_type.update().where(
        resource_type.c.name == "instance_network_interface"
    ).values({'tablename': 'instance_net_int'}))

    resource_type_names = [rt.name for rt in
                           list(bind.execute(resource_type.select()))]

    for name, attributes in legacy.ceilometer_resources.items():
        if name in resource_type_names:
            continue
        tablename = legacy.ceilometer_tablenames.get(name, name)
        text_attributes = json.dumps(attributes)
        op.execute(resource_type.insert().values({
            resource_type.c.attributes: text_attributes,
            resource_type.c.name: name,
            resource_type.c.tablename: tablename,
        }))
