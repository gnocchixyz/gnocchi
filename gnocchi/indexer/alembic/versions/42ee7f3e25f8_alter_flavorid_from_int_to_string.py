"""alter flavorid from int to string

Revision ID: 42ee7f3e25f8
Revises: f7d44b47928
Create Date: 2015-05-10 21:20:24.941263

"""

# revision identifiers, used by Alembic.
revision = '42ee7f3e25f8'
down_revision = 'f7d44b47928'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    for table in ('instance', 'instance_history'):
        op.alter_column(table, "flavor_id",
                        type_=sa.String(length=255),
                        nullable=False)
