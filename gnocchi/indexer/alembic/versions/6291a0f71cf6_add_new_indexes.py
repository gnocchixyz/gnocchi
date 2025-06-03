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

"""empty message

Revision ID: 6291a0f71cf6
Revises: 04eba72e4f90
Create Date: 2025-06-03 11:37:12.161253

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6291a0f71cf6'
down_revision = '04eba72e4f90'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('fk_resource_history_resource_project_id', 'resource_history', ['project_id'], unique=False)
    op.create_index('fk_resource_history_resource_revision_start', 'resource_history', ['revision_start'], unique=False)
    op.create_index('fk_resource_history_resource_revision_end', 'resource_history', ['revision_end'], unique=False)
