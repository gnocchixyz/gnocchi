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

"""Use an id as primary key for archive policy

Revision ID: 71cfe62bcf98
Revises: 1e1a63d3d186
Create Date: 2017-09-24 19:46:22.660503

"""

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils
import uuid


# revision identifiers, used by Alembic.
revision = '71cfe62bcf98'
down_revision = '1e1a63d3d186'
branch_labels = None
depends_on = None

archive_policy_helper = sa.Table(
    'archive_policy',
    sa.MetaData(),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              primary_key=True),
    sa.Column('name', sa.String(length=255), nullable=False, unique=True),
)

metric_helper = sa.Table(
    'metric',
    sa.MetaData(),
    sa.Column('archive_policy_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True)),
    sa.Column('archive_policy_name', sa.String(length=255), nullable=False),
)

apr_helper = sa.Table(
    'archive_policy_rule',
    sa.MetaData(),
    sa.Column('archive_policy_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True)),
    sa.Column('archive_policy_name', sa.String(length=255), nullable=False),
)


def upgrade():
    connection = op.get_bind()
    op.add_column('archive_policy',
                  sa.Column('id',
                            sqlalchemy_utils.types.uuid.UUIDType(
                                binary=True),
                            nullable=True))
    op.add_column('metric',
                  sa.Column('archive_policy_id',
                            sqlalchemy_utils.types.uuid.UUIDType(
                                binary=True),
                            nullable=True))
    op.add_column('archive_policy_rule',
                  sa.Column('archive_policy_id',
                            sqlalchemy_utils.types.uuid.UUIDType(
                                binary=True),
                            nullable=True))

    # Migrate data
    for archive_policy in connection.execute(
            archive_policy_helper.select()):
        ap_id = uuid.uuid4()
        connection.execute(
            archive_policy_helper.update().where(
                archive_policy_helper.c.name == archive_policy.name
            ).values(
                id=ap_id
            )
        )
        for metric in connection.execute(metric_helper.select().where(
                metric_helper.c.archive_policy_name == archive_policy.name)):
            connection.execute(
                metric_helper.update().where(
                    metric_helper.c.archive_policy_name ==
                    metric.archive_policy_name
                ).values(
                    archive_policy_id=ap_id
                )
            )
        for apr in connection.execute(apr_helper.select().where(
                apr_helper.c.archive_policy_name == archive_policy.name)):
            connection.execute(
                apr_helper.update().where(
                    apr_helper.c.archive_policy_name == apr.archive_policy_name
                ).values(
                    archive_policy_id=ap_id
                )
            )
    op.alter_column('archive_policy', 'id', nullable=False,
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True))
    op.alter_column('metric', 'archive_policy_id', nullable=False,
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True))
    op.alter_column('archive_policy_rule', 'archive_policy_id', nullable=False,
                    type_=sqlalchemy_utils.types.uuid.UUIDType(binary=True))

    op.drop_constraint('fk_metric_ap_name_ap_name', 'metric',
                       type_='foreignkey')
    op.drop_constraint('fk_apr_ap_name_ap_name', 'archive_policy_rule',
                       type_='foreignkey')
    if connection and connection.engine.name == "mysql":
        op.drop_constraint('PRIMARY', 'archive_policy', type_='primary')
        # FIXME(0livd) When recreating a primary key named PRIMARY
        # the following error is raised "Incorrect index name 'PRIMARY'"
        op.create_primary_key('archive_policy_pkey', 'archive_policy', ['id'])
    if connection and connection.engine.name == "postgresql":
        op.drop_constraint('archive_policy_pkey', 'archive_policy',
                           type_='primary')
        op.create_primary_key('archive_policy_pkey', 'archive_policy', ['id'])
    op.create_foreign_key(
        'fk_metric_ap_id_ap_id',
        'metric',
        'archive_policy',
        ['archive_policy_id'],
        ['id'],
        ondelete="RESTRICT")
    op.create_foreign_key(
        'fk_apr_ap_id_ap_id',
        'archive_policy_rule',
        'archive_policy',
        ['archive_policy_id'],
        ['id'],
        ondelete="RESTRICT")
    op.create_unique_constraint('uniq_ap_name', 'archive_policy', ['name'])
    op.create_index('ix_ap_name', 'archive_policy', ['name'])

    op.drop_column('metric', 'archive_policy_name')
    op.drop_column('archive_policy_rule', 'archive_policy_name')
