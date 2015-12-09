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

"""create instance_disk and instance_net_int tables

Revision ID: 3901f5ea2b8e
Revises: 42ee7f3e25f8
Create Date: 2015-08-27 17:00:25.092891

"""

# revision identifiers, used by Alembic.
revision = '3901f5ea2b8e'
down_revision = '42ee7f3e25f8'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils


def upgrade():
    for table in ["resource", "resource_history"]:
        op.alter_column(table, "type",
                        type_=sa.Enum('generic', 'instance', 'swift_account',
                                      'volume', 'ceph_account', 'network',
                                      'identity', 'ipmi', 'stack', 'image',
                                      'instance_network_interface',
                                      'instance_disk',
                                      name='resource_type_enum'),
                        nullable=False)

    # NOTE(sileht): postgresql have a builtin ENUM type, so
    # just altering the column won't works.
    # https://bitbucket.org/zzzeek/alembic/issues/270/altering-enum-type
    # Does it break offline migration because we use get_bind() ?

    # NOTE(luogangyi): since we cannot use 'ALTER TYPE' in transaction,
    # we split the 'ALTER TYPE' operation into several steps.
    bind = op.get_bind()
    if bind and bind.engine.name == "postgresql":
        op.execute("ALTER TYPE resource_type_enum RENAME TO \
                    old_resource_type_enum")
        op.execute("CREATE TYPE resource_type_enum AS ENUM \
                       ('generic', 'instance', 'swift_account', \
                        'volume', 'ceph_account', 'network', \
                        'identity', 'ipmi', 'stack', 'image', \
                        'instance_network_interface', 'instance_disk')")
        for table in ["resource", "resource_history"]:
            op.execute("ALTER TABLE %s ALTER COLUMN type TYPE \
                        resource_type_enum USING \
                        type::text::resource_type_enum" % table)
        op.execute("DROP TYPE old_resource_type_enum")

    for table in ['instance_disk', 'instance_net_int']:
        op.create_table(
            table,
            sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                      nullable=False),
            sa.Column('instance_id',
                      sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                      nullable=False),
            sa.Column('name', sa.String(length=255), nullable=False),
            sa.Index('ix_%s_id' % table, 'id', unique=False),
            sa.ForeignKeyConstraint(['id'], ['resource.id'],
                                    name="fk_%s_id_resource_id" % table,
                                    ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            mysql_charset='utf8',
            mysql_engine='InnoDB'
        )

        op.create_table(
            '%s_history' % table,
            sa.Column('instance_id',
                      sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                      nullable=False),
            sa.Column('name', sa.String(length=255), nullable=False),
            sa.Column('revision', sa.Integer(), nullable=False),
            sa.Index('ix_%s_history_revision' % table, 'revision',
                     unique=False),
            sa.ForeignKeyConstraint(['revision'],
                                    ['resource_history.revision'],
                                    name=("fk_%s_history_"
                                          "resource_history_revision") % table,
                                    ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('revision'),
            mysql_charset='utf8',
            mysql_engine='InnoDB'
        )
