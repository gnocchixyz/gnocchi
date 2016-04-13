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

"""shorter_foreign_key

Revision ID: 34c517bcc2dd
Revises: ed9c6ddc5c35
Create Date: 2016-04-13 16:58:42.536431

"""

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision = '34c517bcc2dd'
down_revision = 'ed9c6ddc5c35'
branch_labels = None
depends_on = None


resource_type_helper = sqlalchemy.Table(
    'resource_type',
    sqlalchemy.MetaData(),
    sqlalchemy.Column('tablename', sqlalchemy.String(18), nullable=False)
)

to_rename = [
    ('fk_metric_archive_policy_name_archive_policy_name',
     'fk_metric_ap_name_ap_name',
     'archive_policy', 'name',
     'metric', 'archive_policy_name',
     "RESTRICT"),
    ('fk_resource_history_resource_type_name',
     'fk_rh_resource_type_name',
     'resource_type', 'name', 'resource_history', 'type',
     "RESTRICT"),
    ('fk_resource_history_id_resource_id',
     'fk_rh_id_resource_id',
     'resource', 'id', 'resource_history', 'id',
     "CASCADE"),
    ('fk_archive_policy_rule_archive_policy_name_archive_policy_name',
     'fk_apr_ap_name_ap_name',
     'archive_policy', 'name', 'archive_policy_rule', 'archive_policy_name',
     "RESTRICT")
]


def upgrade():
    connection = op.get_bind()

    insp = sqlalchemy.inspect(connection)

    op.alter_column("resource_type", "tablename",
                    type_=sqlalchemy.String(35),
                    existing_type=sqlalchemy.String(18), nullable=False)

    for rt in connection.execute(resource_type_helper.select()):
        if rt.tablename == "generic":
            continue

        fk_names = [fk['name'] for fk in insp.get_foreign_keys("%s_history" %
                                                               rt.tablename)]
        fk_old = ("fk_%s_history_resource_history_revision" %
                  rt.tablename)
        if fk_old not in fk_names:
            # The table have been created from scratch recently
            fk_old = ("fk_%s_history_revision_resource_history_revision" %
                      rt.tablename)

        fk_new = "fk_%s_h_revision_rh_revision" % rt.tablename
        to_rename.append((fk_old, fk_new, 'resource_history', 'revision',
                          "%s_history" % rt.tablename, 'revision', 'CASCADE'))

    for (fk_old, fk_new, src_table, src_col, dst_table, dst_col, ondelete
         ) in to_rename:
        op.drop_constraint(fk_old, dst_table, type_="foreignkey")
        op.create_foreign_key(fk_new, dst_table, src_table,
                              [dst_col], [src_col], ondelete=ondelete)
