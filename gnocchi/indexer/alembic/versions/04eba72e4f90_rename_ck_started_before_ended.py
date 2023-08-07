# Copyright 2019 The Gnocchi Developers
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

"""rename ck_started_before_ended

Revision ID: 04eba72e4f90
Revises: 1e1a63d3d186
Create Date: 2019-10-01 11:19:38.865522

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '04eba72e4f90'
down_revision = '1e1a63d3d186'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    for table in ("resource", "resource_history"):
        existing_cks = [
            c['name'] for c in inspector.get_check_constraints(table)
        ]
        if "ck_started_before_ended" in existing_cks:
            # Drop non-uniquely named check constraints
            # for consistency across DB types.
            op.drop_constraint("ck_started_before_ended",
                               table,
                               type_="check")

        new_ck_name = "ck_{}_started_before_ended".format(table)
        if new_ck_name not in existing_cks:
            # Re-create check constraint with unique name
            # if needed
            op.create_check_constraint(new_ck_name,
                                       table,
                                       "started_at <= ended_at")
