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

"""Add attributes to resource_type

Revision ID: d24877c22ab0
Revises: 0718ed97e5b3
Create Date: 2016-01-19 22:45:06.431190

"""

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils as sa_utils


# revision identifiers, used by Alembic.
revision = 'd24877c22ab0'
down_revision = '0718ed97e5b3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("resource_type",
                  sa.Column('attributes', sa_utils.JSONType(),))
