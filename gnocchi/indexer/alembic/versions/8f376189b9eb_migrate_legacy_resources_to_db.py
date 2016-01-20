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

"""Migrate legacy resources to DB

Revision ID: 8f376189b9eb
Revises: d24877c22ab0
Create Date: 2016-01-20 15:03:28.115656

"""
import json

from alembic import op
import sqlalchemy as sa

from gnocchi.indexer import sqlalchemy_legacy_resources as legacy

# revision identifiers, used by Alembic.
revision = '8f376189b9eb'
down_revision = 'd24877c22ab0'
branch_labels = None
depends_on = None


def upgrade():
    resource_type = sa.Table(
        'resource_type', sa.MetaData(),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('attributes', sa.Text, nullable=False)
    )

    for name, attributes in legacy.ceilometer_resources.items():
        text_attributes = json.dumps(attributes)
        op.execute(resource_type.update().where(
            resource_type.c.name == name
        ).values({resource_type.c.attributes: text_attributes}))
