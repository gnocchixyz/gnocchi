# Copyright 2017 The Gnocchi Developers
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

"""Use cascade update for metric table's ap foreign key

Revision ID: 815b184585a5
Revises: 1e1a63d3d186
Create Date: 2017-09-25 17:38:23.954423

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = '815b184585a5'
down_revision = '1e1a63d3d186'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("fk_metric_ap_name_ap_name",
                       "metric",
                       type_="foreignkey")
    op.create_foreign_key("fk_metric_ap_name_ap_name",
                          "metric", "archive_policy",
                          ["archive_policy_name"],
                          ["name"],
                          ondelete='RESTRICT',
                          onupdate="CASCADE")
