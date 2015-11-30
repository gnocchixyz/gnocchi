# Copyright 2015 OpenStack Foundation
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

"""allow image_ref to be null

Revision ID: 469b308577a9
Revises: 39b7d449d46a
Create Date: 2015-11-29 00:23:39.998256

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '469b308577a9'
down_revision = '39b7d449d46a'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('instance', 'image_ref',
                    existing_type=sa.String(length=255),
                    nullable=True)
    op.alter_column('instance_history', 'image_ref',
                    existing_type=sa.String(length=255),
                    nullable=True)
