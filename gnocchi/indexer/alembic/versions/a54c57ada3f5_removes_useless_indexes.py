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

"""merges primarykey and indexes

Revision ID: a54c57ada3f5
Revises: 1c2c61ac1f4c
Create Date: 2016-02-04 09:09:23.180955

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = 'a54c57ada3f5'
down_revision = '1c2c61ac1f4c'
branch_labels = None
depends_on = None

resource_tables = [(t, "id") for t in [
    "instance",
    "instance_disk",
    "instance_net_int",
    "swift_account",
    "volume",
    "ceph_account",
    "network",
    "identity",
    "ipmi",
    "stack",
    "image"
]]
history_tables = [("%s_history" % t, "revision")
                  for t, c in resource_tables]
other_tables = [("metric", "id"), ("archive_policy", "name"),
                ("archive_policy_rule", "name"),
                ("resource", "id"),
                ("resource_history", "id")]


def upgrade():
    bind = op.get_bind()
    # NOTE(sileht): mysql can't delete an index on a foreign key
    # even this one is not the index used by the foreign key itself...
    # In our case we have two indexes fk_resource_history_id_resource_id and
    # and ix_resource_history_id, we want to delete only the second, but mysql
    # can't do that with a simple DROP INDEX ix_resource_history_id...
    # so we have to remove the constraint and put it back...
    if bind.engine.name == "mysql":
        op.drop_constraint("fk_resource_history_id_resource_id",
                           type_="foreignkey", table_name="resource_history")

    for table, colname in resource_tables + history_tables + other_tables:
        op.drop_index("ix_%s_%s" % (table, colname), table_name=table)

    if bind.engine.name == "mysql":
        op.create_foreign_key("fk_resource_history_id_resource_id",
                              "resource_history", "resource", ["id"], ["id"],
                              ondelete="CASCADE")
