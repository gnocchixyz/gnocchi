# -*- encoding: utf-8 -*-
#
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

"""mysql_float_to_timestamp

Revision ID: 5c4f93e5bb4
Revises: 7e6f9d542f8b
Create Date: 2016-07-25 15:36:36.469847

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

from gnocchi.indexer import sqlalchemy_base

# revision identifiers, used by Alembic.
revision = '5c4f93e5bb4'
down_revision = '27d2a1d205ff'
branch_labels = None
depends_on = None


timestamp_default = '1000-01-01 00:00:00.000000'


def upgrade():
    bind = op.get_bind()

    if bind.engine.name == "mysql":
        op.execute("SET time_zone = '+00:00'")

    # NOTE(jd) So that crappy engine that is MySQL does not have "ALTER
    # TABLE … USING …". We need to copy everything and convert…
    for table_name, column_name in (("resource", "started_at"),
                                    ("resource", "ended_at"),
                                    ("resource", "revision_start"),
                                    ("resource_history", "started_at"),
                                    ("resource_history", "ended_at"),
                                    ("resource_history", "revision_start"),
                                    ("resource_history", "revision_end"),
                                    ("resource_type", "updated_at")):

        nullable = column_name == "ended_at"
        server_default = None if nullable else timestamp_default

        if bind.engine.name == "mysql":
            existing_col = sa.Column(
                column_name,
                sa.types.DECIMAL(precision=20, scale=6, asdecimal=True),
                nullable=nullable)
            temp_col = sa.Column(
                column_name + "_ts",
                sqlalchemy_base.TimestampUTC(),
                nullable=nullable,
                server_default=server_default,
            )
            op.add_column(table_name, temp_col)
            t = sa.sql.table(table_name, existing_col, temp_col)
            op.execute(t.update().values(
                **{column_name + "_ts": func.from_unixtime(existing_col)}))
            op.drop_column(table_name, column_name)
            op.alter_column(table_name,
                            column_name + "_ts",
                            existing_type=sqlalchemy_base.TimestampUTC(),
                            existing_nullable=nullable,
                            existing_server_default=server_default,
                            server_default=server_default,
                            new_column_name=column_name)
        else:
            op.alter_column(
                table_name,
                column_name,
                existing_type=sqlalchemy_base.TimestampUTC(),
                existing_nullable=nullable,
                existing_server_default=None,
                server_default=None if nullable else timestamp_default)
