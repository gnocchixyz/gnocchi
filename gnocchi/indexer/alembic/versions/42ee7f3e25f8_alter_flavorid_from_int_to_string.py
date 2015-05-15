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

"""alter flavorid from int to string

Revision ID: 42ee7f3e25f8
Revises: f7d44b47928
Create Date: 2015-05-10 21:20:24.941263

"""

# revision identifiers, used by Alembic.
revision = '42ee7f3e25f8'
down_revision = 'f7d44b47928'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    for table in ('instance', 'instance_history'):
        op.alter_column(table, "flavor_id",
                        type_=sa.String(length=255),
                        nullable=False)
