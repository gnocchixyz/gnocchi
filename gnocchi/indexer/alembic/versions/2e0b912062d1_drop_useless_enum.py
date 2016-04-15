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

"""drop_useless_enum

Revision ID: 2e0b912062d1
Revises: 34c517bcc2dd
Create Date: 2016-04-15 07:29:38.492237

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = '2e0b912062d1'
down_revision = '34c517bcc2dd'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind and bind.engine.name == "postgresql":
        # NOTE(sileht): we use IF exists because if the database have
        # been created from scratch with 2.1 the enum doesn't exists
        op.execute("DROP TYPE IF EXISTS resource_type_enum")
