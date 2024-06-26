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

"""create metric truncation status column

Revision ID: 18fff4509e3e
Revises: 04eba72e4f90
Create Date: 2024-04-24 09:16:00

"""
import datetime

from alembic import op
from sqlalchemy.sql import func

import sqlalchemy

# revision identifiers, used by Alembic.
revision = '18fff4509e3e'
down_revision = '04eba72e4f90'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "metric", sqlalchemy.Column(
            "needs_raw_data_truncation", sqlalchemy.Boolean,
            nullable=False, default=True,
            server_default=sqlalchemy.sql.true()))
