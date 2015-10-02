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

"""create metric status column

Revision ID: 39b7d449d46a
Revises: 3901f5ea2b8e
Create Date: 2015-09-16 13:25:34.249237

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '39b7d449d46a'
down_revision = '3901f5ea2b8e'
branch_labels = None
depends_on = None


def upgrade():
    enum = sa.Enum("active", "delete", name="metric_status_enum")
    enum.create(op.get_bind(), checkfirst=False)
    op.add_column("metric",
                  sa.Column('status', enum,
                            nullable=False,
                            server_default="active"))
    op.create_index('ix_metric_status', 'metric', ['status'], unique=False)

    op.drop_constraint("fk_metric_resource_id_resource_id",
                       "metric", type_="foreignkey")
    op.create_foreign_key("fk_metric_resource_id_resource_id",
                          "metric", "resource",
                          ("resource_id",), ("id",),
                          ondelete="SET NULL")
