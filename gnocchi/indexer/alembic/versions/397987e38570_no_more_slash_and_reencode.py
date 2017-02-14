# Copyright 2017 OpenStack Foundation
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

"""Remove slashes from original resource IDs, recompute their id with creator

Revision ID: 397987e38570
Revises: aba5a217ca9b
Create Date: 2017-01-11 16:32:40.421758

"""
import uuid

from alembic import op
import six
import sqlalchemy as sa
import sqlalchemy_utils

from gnocchi import utils

# revision identifiers, used by Alembic.
revision = '397987e38570'
down_revision = 'aba5a217ca9b'
branch_labels = None
depends_on = None

resource_type_table = sa.Table(
    'resource_type',
    sa.MetaData(),
    sa.Column('name', sa.String(255), nullable=False),
    sa.Column('tablename', sa.String(35), nullable=False)
)

resource_table = sa.Table(
    'resource',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(),
              nullable=False),
    sa.Column('original_resource_id', sa.String(255)),
    sa.Column('type', sa.String(255)),
    sa.Column('creator', sa.String(255))
)

resourcehistory_table = sa.Table(
    'resource_history',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(),
              nullable=False),
    sa.Column('original_resource_id', sa.String(255))
)

metric_table = sa.Table(
    'metric',
    sa.MetaData(),
    sa.Column('id',
              sqlalchemy_utils.types.uuid.UUIDType(),
              nullable=False),
    sa.Column('name', sa.String(255)),
    sa.Column('resource_id', sqlalchemy_utils.types.uuid.UUIDType())

)


uuidtype = sqlalchemy_utils.types.uuid.UUIDType()


def upgrade():
    connection = op.get_bind()

    resource_type_tables = {}
    resource_type_tablenames = dict(
        (rt.name, rt.tablename)
        for rt in connection.execute(resource_type_table.select())
        if rt.tablename != "generic"
    )

    op.drop_constraint("fk_metric_resource_id_resource_id", "metric",
                       type_="foreignkey")
    for name, table in resource_type_tablenames.items():
        op.drop_constraint("fk_%s_id_resource_id" % table, table,
                           type_="foreignkey")

        resource_type_tables[name] = sa.Table(
            table,
            sa.MetaData(),
            sa.Column('id',
                      sqlalchemy_utils.types.uuid.UUIDType(),
                      nullable=False),
        )

    for resource in connection.execute(resource_table.select()):

        if resource.original_resource_id is None:
            # statsd resource has no original_resource_id and is NULL
            continue

        try:
            orig_as_uuid = uuid.UUID(str(resource.original_resource_id))
        except ValueError:
            pass
        else:
            if orig_as_uuid == resource.id:
                continue

        new_original_resource_id = resource.original_resource_id.replace(
            '/', '_')
        if six.PY2:
            new_original_resource_id = new_original_resource_id.encode('utf-8')
        new_id = sa.literal(uuidtype.process_bind_param(
            str(utils.ResourceUUID(
                new_original_resource_id, resource.creator)),
            connection.dialect))

        # resource table
        connection.execute(
            resource_table.update().where(
                resource_table.c.id == resource.id
            ).values(
                id=new_id,
                original_resource_id=new_original_resource_id
            )
        )
        # resource history table
        connection.execute(
            resourcehistory_table.update().where(
                resourcehistory_table.c.id == resource.id
            ).values(
                id=new_id,
                original_resource_id=new_original_resource_id
            )
        )

        if resource.type != "generic":
            rtable = resource_type_tables[resource.type]

            # resource table (type)
            connection.execute(
                rtable.update().where(
                    rtable.c.id == resource.id
                ).values(id=new_id)
            )

        # Metric
        connection.execute(
            metric_table.update().where(
                metric_table.c.resource_id == resource.id
            ).values(
                resource_id=new_id
            )
        )

    for (name, table) in resource_type_tablenames.items():
        op.create_foreign_key("fk_%s_id_resource_id" % table,
                              table, "resource",
                              ("id",), ("id",),
                              ondelete="CASCADE")

    op.create_foreign_key("fk_metric_resource_id_resource_id",
                          "metric", "resource",
                          ("resource_id",), ("id",),
                          ondelete="SET NULL")

    for metric in connection.execute(metric_table.select().where(
            metric_table.c.name.like("%/%"))):
        connection.execute(
            metric_table.update().where(
                metric_table.c.id == metric.id
            ).values(
                name=metric.name.replace('/', '_'),
            )
        )
