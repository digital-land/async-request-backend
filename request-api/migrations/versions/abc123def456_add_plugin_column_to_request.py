"""add plugin column to request table

Revision ID: abc123def456
Revises: d45c986e2727
Create Date: 2025-10-07 13:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "abc123def456"
down_revision = "d45c986e2727"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("request", sa.Column("plugin", sa.String(), nullable=True))


def downgrade():
    op.drop_column("request", "plugin")
