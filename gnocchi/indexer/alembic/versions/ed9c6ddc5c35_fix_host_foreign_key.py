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

"""fix_host_foreign_key

Revision ID: ed9c6ddc5c35
Revises: ffc7bbeec0b0
Create Date: 2016-04-15 06:25:34.649934

"""

from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = 'ed9c6ddc5c35'
down_revision = 'ffc7bbeec0b0'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    insp = inspect(conn)
    fk_names = [fk['name'] for fk in insp.get_foreign_keys('host')]
    if ("fk_hypervisor_id_resource_id" not in fk_names and
            "fk_host_id_resource_id" in fk_names):
        # NOTE(sileht): we are already good, the BD have been created from
        # scratch after "a54c57ada3f5"
        return

    op.drop_constraint("fk_hypervisor_id_resource_id", "host",
                       type_="foreignkey")
    op.drop_constraint("fk_hypervisor_history_resource_history_revision",
                       "host_history", type_="foreignkey")
    op.create_foreign_key("fk_host_id_resource_id", "host", "resource",
                          ["id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key("fk_host_history_resource_history_revision",
                          "host_history", "resource_history",
                          ["revision"], ["revision"], ondelete="CASCADE")
