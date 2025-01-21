# Copyright 2025 The Gnocchi Developers
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

"""Increase original resource ID max length

Revision ID: 94544ca86c7e
Revises: 18fff4509e3e
Create Date: 2025-01-15 22:12:53.822748

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '94544ca86c7e'
down_revision = '18fff4509e3e'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("resource", "original_resource_id",
                    type_=sa.String(289),
                    nullable=False,
                    existing_type=sa.String(255),
                    existing_nullable=False)
    op.alter_column("resource_history", "original_resource_id",
                    type_=sa.String(289),
                    nullable=False,
                    existing_type=sa.String(255),
                    existing_nullable=False)
