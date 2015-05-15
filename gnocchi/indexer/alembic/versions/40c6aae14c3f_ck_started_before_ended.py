#
# Copyright 2015 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""ck_started_before_ended

Revision ID: 40c6aae14c3f
Revises: 1c98ac614015
Create Date: 2015-04-28 16:35:11.999144

"""

# revision identifiers, used by Alembic.
revision = '40c6aae14c3f'
down_revision = '1c98ac614015'
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    op.create_check_constraint("ck_started_before_ended",
                               "resource",
                               "started_at <= ended_at")
    op.create_check_constraint("ck_started_before_ended",
                               "resource_history",
                               "started_at <= ended_at")
