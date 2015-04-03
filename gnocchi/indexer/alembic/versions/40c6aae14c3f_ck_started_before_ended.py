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
