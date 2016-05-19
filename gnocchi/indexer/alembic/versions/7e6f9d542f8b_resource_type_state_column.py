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

"""resource_type state column

Revision ID: 7e6f9d542f8b
Revises: c62df18bf4ee
Create Date: 2016-05-19 16:52:58.939088

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7e6f9d542f8b'
down_revision = 'c62df18bf4ee'
branch_labels = None
depends_on = None


def upgrade():
    states = ("active", "creating", "creation_error", "deleting",
              "deletion_error")
    enum = sa.Enum(*states, name="resource_type_state_enum")
    enum.create(op.get_bind(), checkfirst=False)
    op.add_column("resource_type",
                  sa.Column('state', enum, nullable=False,
                            server_default="creating"))
    rt = sa.sql.table('resource_type', sa.sql.column('state', enum))
    op.execute(rt.update().values(state="active"))
