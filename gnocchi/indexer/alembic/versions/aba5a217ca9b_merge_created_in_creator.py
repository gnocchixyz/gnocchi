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

"""merge_created_in_creator

Revision ID: aba5a217ca9b
Revises: 5c4f93e5bb4
Create Date: 2016-12-06 17:40:25.344578

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aba5a217ca9b'
down_revision = '5c4f93e5bb4'
branch_labels = None
depends_on = None


def upgrade():
    for table_name in ("resource", "resource_history", "metric"):
        creator_col = sa.Column("creator", sa.String(255))
        created_by_user_id_col = sa.Column("created_by_user_id",
                                           sa.String(255))
        created_by_project_id_col = sa.Column("created_by_project_id",
                                              sa.String(255))
        op.add_column(table_name, creator_col)
        t = sa.sql.table(
            table_name, creator_col,
            created_by_user_id_col, created_by_project_id_col)
        op.execute(
            t.update().values(
                creator=(
                    created_by_user_id_col + ":" + created_by_project_id_col
                )).where((created_by_user_id_col is not None)
                         | (created_by_project_id_col is not None)))
        op.drop_column(table_name, "created_by_user_id")
        op.drop_column(table_name, "created_by_project_id")
