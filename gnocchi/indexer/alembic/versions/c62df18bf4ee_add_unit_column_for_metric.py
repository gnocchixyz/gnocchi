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

"""add unit column for metric

Revision ID: c62df18bf4ee
Revises: 2e0b912062d1
Create Date: 2016-05-04 12:31:25.350190

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c62df18bf4ee'
down_revision = '2e0b912062d1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('metric', sa.Column('unit',
                                      sa.String(length=31),
                                      nullable=True))
