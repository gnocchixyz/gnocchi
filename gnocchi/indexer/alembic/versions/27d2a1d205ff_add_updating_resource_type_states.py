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

"""Add updating resource type states

Revision ID: 27d2a1d205ff
Revises: 7e6f9d542f8b
Create Date: 2016-08-31 14:05:34.316496

"""

from alembic import op
import sqlalchemy as sa

from gnocchi.indexer import sqlalchemy_types
from gnocchi import utils

# revision identifiers, used by Alembic.
revision = '27d2a1d205ff'
down_revision = '7e6f9d542f8b'
branch_labels = None
depends_on = None


resource_type = sa.sql.table(
    'resource_type',
    sa.sql.column('updated_at', sqlalchemy_types.PreciseTimestamp()))

state_enum = sa.Enum("active", "creating",
                     "creation_error", "deleting",
                     "deletion_error", "updating",
                     "updating_error",
                     name="resource_type_state_enum")


def upgrade():

    op.alter_column('resource_type', 'state',
                    type_=state_enum,
                    nullable=False,
                    server_default=None)

    # NOTE(sileht): postgresql have a builtin ENUM type, so
    # just altering the column won't works.
    # https://bitbucket.org/zzzeek/alembic/issues/270/altering-enum-type
    # Does it break offline migration because we use get_bind() ?

    # NOTE(luogangyi): since we cannot use 'ALTER TYPE' in transaction,
    # we split the 'ALTER TYPE' operation into several steps.
    bind = op.get_bind()
    if bind and bind.engine.name == "postgresql":
        op.execute("ALTER TYPE resource_type_state_enum RENAME TO \
                    old_resource_type_state_enum")
        op.execute("CREATE TYPE resource_type_state_enum AS ENUM \
                       ('active', 'creating', 'creation_error', \
                        'deleting', 'deletion_error', 'updating', \
                        'updating_error')")
        op.execute("ALTER TABLE resource_type ALTER COLUMN state TYPE \
                   resource_type_state_enum USING \
                   state::text::resource_type_state_enum")
        op.execute("DROP TYPE old_resource_type_state_enum")

    # NOTE(sileht): we can't alter type with server_default set on
    # postgresql...
    op.alter_column('resource_type', 'state',
                    type_=state_enum,
                    nullable=False,
                    server_default="creating")
    op.add_column("resource_type",
                  sa.Column("updated_at",
                            sqlalchemy_types.PreciseTimestamp(),
                            nullable=True))

    op.execute(resource_type.update().values({'updated_at': utils.utcnow()}))
    op.alter_column("resource_type", "updated_at",
                    type_=sqlalchemy_types.PreciseTimestamp(),
                    nullable=False)
