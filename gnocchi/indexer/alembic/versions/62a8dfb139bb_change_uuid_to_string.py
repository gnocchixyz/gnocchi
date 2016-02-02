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

"""Change uuid to string

Revision ID: 62a8dfb139bb
Revises: 1f21cbdd6bc2
Create Date: 2016-01-20 11:57:45.954607

"""

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils


# revision identifiers, used by Alembic.
revision = '62a8dfb139bb'
down_revision = '1f21cbdd6bc2'
branch_labels = None
depends_on = None

resourcehelper = sa.Table(
    'resource',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('tmp_created_by_user_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_created_by_project_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_user_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_project_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('created_by_user_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('created_by_project_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('user_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('project_id',
              sa.String(length=255),
              nullable=True),
)

resourcehistoryhelper = sa.Table(
    'resource_history',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('tmp_created_by_user_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_created_by_project_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_user_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_project_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('created_by_user_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('created_by_project_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('user_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('project_id',
              sa.String(length=255),
              nullable=True),
)

metrichelper = sa.Table(
    'metric',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=False),
    sa.Column('tmp_created_by_user_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('tmp_created_by_project_id',
              sqlalchemy_utils.types.uuid.UUIDType(binary=True),
              nullable=True),
    sa.Column('created_by_user_id',
              sa.String(length=255),
              nullable=True),
    sa.Column('created_by_project_id',
              sa.String(length=255),
              nullable=True),
)


def upgrade():
    connection = op.get_bind()

    # Rename user/project fields to tmp_*
    op.alter_column('metric', 'created_by_project_id',
                    new_column_name='tmp_created_by_project_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('metric', 'created_by_user_id',
                    new_column_name='tmp_created_by_user_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource', 'created_by_project_id',
                    new_column_name='tmp_created_by_project_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource', 'created_by_user_id',
                    new_column_name='tmp_created_by_user_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource', 'project_id',
                    new_column_name='tmp_project_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource', 'user_id',
                    new_column_name='tmp_user_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource_history', 'created_by_project_id',
                    new_column_name='tmp_created_by_project_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource_history', 'created_by_user_id',
                    new_column_name='tmp_created_by_user_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource_history', 'project_id',
                    new_column_name='tmp_project_id',
                    existing_type=sa.BINARY(length=16))
    op.alter_column('resource_history', 'user_id',
                    new_column_name='tmp_user_id',
                    existing_type=sa.BINARY(length=16))

    # Add new user/project fields as strings
    op.add_column('metric',
                  sa.Column('created_by_project_id',
                            sa.String(length=255), nullable=True))
    op.add_column('metric',
                  sa.Column('created_by_user_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource',
                  sa.Column('created_by_project_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource',
                  sa.Column('created_by_user_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource',
                  sa.Column('project_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource',
                  sa.Column('user_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource_history',
                  sa.Column('created_by_project_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource_history',
                  sa.Column('created_by_user_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource_history',
                  sa.Column('project_id',
                            sa.String(length=255), nullable=True))
    op.add_column('resource_history',
                  sa.Column('user_id',
                            sa.String(length=255), nullable=True))

    # Migrate data
    for tablehelper in [resourcehelper, resourcehistoryhelper]:
        for resource in connection.execute(tablehelper.select()):
            if resource.tmp_created_by_project_id:
                created_by_project_id = \
                    str(resource.tmp_created_by_project_id).replace('-', '')
            else:
                created_by_project_id = None
            if resource.tmp_created_by_user_id:
                created_by_user_id = \
                    str(resource.tmp_created_by_user_id).replace('-', '')
            else:
                created_by_user_id = None
            if resource.tmp_project_id:
                project_id = str(resource.tmp_project_id).replace('-', '')
            else:
                project_id = None
            if resource.tmp_user_id:
                user_id = str(resource.tmp_user_id).replace('-', '')
            else:
                user_id = None

            connection.execute(
                tablehelper.update().where(
                    tablehelper.c.id == resource.id
                ).values(
                    created_by_project_id=created_by_project_id,
                    created_by_user_id=created_by_user_id,
                    project_id=project_id,
                    user_id=user_id,
                )
            )
    for metric in connection.execute(metrichelper.select()):
        if resource.tmp_created_by_project_id:
            created_by_project_id = \
                str(resource.tmp_created_by_project_id).replace('-', '')
        else:
            created_by_project_id = None
        if resource.tmp_created_by_user_id:
            created_by_user_id = \
                str(resource.tmp_created_by_user_id).replace('-', '')
        else:
            created_by_user_id = None
        connection.execute(
            metrichelper.update().where(
                metrichelper.c.id == metric.id
            ).values(
                created_by_project_id=created_by_project_id,
                created_by_user_id=created_by_user_id,
            )
        )

    # Delete temp fields
    op.drop_column('metric', 'tmp_created_by_project_id')
    op.drop_column('metric', 'tmp_created_by_user_id')
    op.drop_column('resource', 'tmp_created_by_project_id')
    op.drop_column('resource', 'tmp_created_by_user_id')
    op.drop_column('resource', 'tmp_project_id')
    op.drop_column('resource', 'tmp_user_id')
    op.drop_column('resource_history', 'tmp_created_by_project_id')
    op.drop_column('resource_history', 'tmp_created_by_user_id')
    op.drop_column('resource_history', 'tmp_project_id')
    op.drop_column('resource_history', 'tmp_user_id')
