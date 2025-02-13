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

"""Create last measure push timestamp column
Revision ID: f89ed2e3c2ec
Revises: 94544ca86c7e
Create Date: 2024-04-24 09:16:00
"""

import datetime
from alembic import op

import sqlalchemy

# revision identifiers, used by Alembic.
revision = 'f89ed2e3c2ec'
down_revision = '94544ca86c7e'
branch_labels = None
depends_on = None


def upgrade():
    sql_column = sqlalchemy.Column(
            "last_measure_timestamp", sqlalchemy.DateTime,
            nullable=False, default=datetime.datetime.utcnow(),
            server_default=sqlalchemy.sql.func.current_timestamp())
    op.add_column(
        "metric", sql_column)
