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

"""add original resource id column

Revision ID: 1c2c61ac1f4c
Revises: 1f21cbdd6bc2
Create Date: 2016-01-27 05:57:48.909012

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '1c2c61ac1f4c'
down_revision = '62a8dfb139bb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('resource', sa.Column('original_resource_id',
                                        sa.String(length=255),
                                        nullable=True))
    op.add_column('resource_history', sa.Column('original_resource_id',
                                                sa.String(length=255),
                                                nullable=True))
