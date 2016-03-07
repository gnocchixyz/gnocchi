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

"""create host tables

Revision ID: 9901e5ea4b6e
Revises: a54c57ada3f5
Create Date: 2015-12-15 17:20:25.092891

"""

# revision identifiers, used by Alembic.
revision = '9901e5ea4b6e'
down_revision = 'a54c57ada3f5'
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
                                      'host', 'host_disk',
                                      'host_network_interface',
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
                        'instance_network_interface', 'instance_disk', \
                        'host', 'host_disk', \
                        'host_network_interface')")
        for table in ["resource", "resource_history"]:
            op.execute("ALTER TABLE %s ALTER COLUMN type TYPE \
                        resource_type_enum USING \
                        type::text::resource_type_enum" % table)
        op.execute("DROP TYPE old_resource_type_enum")

    op.create_table(
        'host',
        sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                  nullable=False),
        sa.Column('host_name', sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(['id'], ['resource.id'],
                                name="fk_hypervisor_id_resource_id",
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )

    op.create_table(
        'host_history',
        sa.Column('host_name', sa.String(length=255), nullable=False),
        sa.Column('revision', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['revision'],
                                ['resource_history.revision'],
                                name=("fk_hypervisor_history_"
                                      "resource_history_revision"),
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('revision'),
        mysql_charset='utf8',
        mysql_engine='InnoDB'
    )

    for table in ['host_disk', 'host_net_int']:
        op.create_table(
            table,
            sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=True),
                      nullable=False),
            sa.Column('host_name', sa.String(length=255), nullable=False),
            sa.Column('device_name', sa.String(length=255), nullable=True),
            sa.ForeignKeyConstraint(['id'], ['resource.id'],
                                    name="fk_%s_id_resource_id" % table,
                                    ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            mysql_charset='utf8',
            mysql_engine='InnoDB'
        )

        op.create_table(
            '%s_history' % table,
            sa.Column('host_name', sa.String(length=255), nullable=False),
            sa.Column('device_name', sa.String(length=255), nullable=True),
            sa.Column('revision', sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(['revision'],
                                    ['resource_history.revision'],
                                    name=("fk_%s_history_"
                                          "resource_history_revision") % table,
                                    ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('revision'),
            mysql_charset='utf8',
            mysql_engine='InnoDB'
        )
