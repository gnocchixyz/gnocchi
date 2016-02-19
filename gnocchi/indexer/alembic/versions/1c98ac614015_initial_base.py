# flake8: noqa
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

"""Initial base for Gnocchi 1.0.0

Revision ID: 1c98ac614015
Revises: 
Create Date: 2015-04-27 16:05:13.530625

"""

# revision identifiers, used by Alembic.
revision = '1c98ac614015'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import sqlalchemy_utils

import gnocchi.indexer.sqlalchemy_base


def upgrade():
    op.create_table('resource',
    sa.Column('type', sa.Enum('generic', 'instance', 'swift_account', 'volume', 'ceph_account', 'network', 'identity', 'ipmi', 'stack', 'image', name='resource_type_enum'), nullable=False),
    sa.Column('created_by_user_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('created_by_project_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('started_at', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=False),
    sa.Column('revision_start', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=False),
    sa.Column('ended_at', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=True),
    sa.Column('user_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('project_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_resource_id', 'resource', ['id'], unique=False)
    op.create_table('archive_policy',
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('back_window', sa.Integer(), nullable=False),
    sa.Column('definition', gnocchi.indexer.sqlalchemy_base.ArchivePolicyDefinitionType(), nullable=False),
    sa.Column('aggregation_methods', gnocchi.indexer.sqlalchemy_base.SetType(), nullable=False),
    sa.PrimaryKeyConstraint('name'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_archive_policy_name', 'archive_policy', ['name'], unique=False)
    op.create_table('volume',
    sa.Column('display_name', sa.String(length=255), nullable=False),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_volume_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_volume_id', 'volume', ['id'], unique=False)
    op.create_table('instance',
    sa.Column('flavor_id', sa.Integer(), nullable=False),
    sa.Column('image_ref', sa.String(length=255), nullable=False),
    sa.Column('host', sa.String(length=255), nullable=False),
    sa.Column('display_name', sa.String(length=255), nullable=False),
    sa.Column('server_group', sa.String(length=255), nullable=True),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_instance_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_id', 'instance', ['id'], unique=False)
    op.create_table('stack',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_stack_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_stack_id', 'stack', ['id'], unique=False)
    op.create_table('archive_policy_rule',
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('archive_policy_name', sa.String(length=255), nullable=False),
    sa.Column('metric_pattern', sa.String(length=255), nullable=False),
    sa.ForeignKeyConstraint(['archive_policy_name'], ['archive_policy.name'], name="fk_archive_policy_rule_archive_policy_name_archive_policy_name", ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('name'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_archive_policy_rule_name', 'archive_policy_rule', ['name'], unique=False)
    op.create_table('swift_account',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_swift_account_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_swift_account_id', 'swift_account', ['id'], unique=False)
    op.create_table('ceph_account',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_ceph_account_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_ceph_account_id', 'ceph_account', ['id'], unique=False)
    op.create_table('ipmi',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_ipmi_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_ipmi_id', 'ipmi', ['id'], unique=False)
    op.create_table('image',
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('container_format', sa.String(length=255), nullable=False),
    sa.Column('disk_format', sa.String(length=255), nullable=False),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_image_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_image_id', 'image', ['id'], unique=False)
    op.create_table('resource_history',
    sa.Column('type', sa.Enum('generic', 'instance', 'swift_account', 'volume', 'ceph_account', 'network', 'identity', 'ipmi', 'stack', 'image', name='resource_type_enum'), nullable=False),
    sa.Column('created_by_user_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('created_by_project_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('started_at', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=False),
    sa.Column('revision_start', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=False),
    sa.Column('ended_at', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=True),
    sa.Column('user_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('project_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.Column('revision_end', gnocchi.indexer.sqlalchemy_base.PreciseTimestamp(), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_resource_history_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_resource_history_id', 'resource_history', ['id'], unique=False)
    op.create_table('identity',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_identity_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_identity_id', 'identity', ['id'], unique=False)
    op.create_table('network',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['resource.id'], name="fk_network_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_network_id', 'network', ['id'], unique=False)
    op.create_table('metric',
    sa.Column('id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
    sa.Column('archive_policy_name', sa.String(length=255), nullable=False),
    sa.Column('created_by_user_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('created_by_project_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('resource_id', sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.ForeignKeyConstraint(['archive_policy_name'], ['archive_policy.name'], name="fk_metric_archive_policy_name_archive_policy_name", ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['resource_id'], ['resource.id'], name="fk_metric_resource_id_resource_id", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('resource_id', 'name', name='uniq_metric0resource_id0name'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_metric_id', 'metric', ['id'], unique=False)
    op.create_table('identity_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_identity_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_identity_history_revision', 'identity_history', ['revision'], unique=False)
    op.create_table('instance_history',
    sa.Column('flavor_id', sa.Integer(), nullable=False),
    sa.Column('image_ref', sa.String(length=255), nullable=False),
    sa.Column('host', sa.String(length=255), nullable=False),
    sa.Column('display_name', sa.String(length=255), nullable=False),
    sa.Column('server_group', sa.String(length=255), nullable=True),
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_instance_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_instance_history_revision', 'instance_history', ['revision'], unique=False)
    op.create_table('network_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_network_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_network_history_revision', 'network_history', ['revision'], unique=False)
    op.create_table('swift_account_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_swift_account_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_swift_account_history_revision', 'swift_account_history', ['revision'], unique=False)
    op.create_table('ceph_account_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_ceph_account_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_ceph_account_history_revision', 'ceph_account_history', ['revision'], unique=False)
    op.create_table('ipmi_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_ipmi_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_ipmi_history_revision', 'ipmi_history', ['revision'], unique=False)
    op.create_table('image_history',
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('container_format', sa.String(length=255), nullable=False),
    sa.Column('disk_format', sa.String(length=255), nullable=False),
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_image_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_image_history_revision', 'image_history', ['revision'], unique=False)
    op.create_table('stack_history',
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_stack_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_stack_history_revision', 'stack_history', ['revision'], unique=False)
    op.create_table('volume_history',
    sa.Column('display_name', sa.String(length=255), nullable=False),
    sa.Column('revision', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['revision'], ['resource_history.revision'], name="fk_volume_history_resource_history_revision", ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('revision'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_index('ix_volume_history_revision', 'volume_history', ['revision'], unique=False)
