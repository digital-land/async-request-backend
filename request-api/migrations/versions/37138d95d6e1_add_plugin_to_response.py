"""add plugin to response

Revision ID: 37138d95d6e1
Revises: d45c986e2727
Create Date: 2025-12-09 16:35:05.584177

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "37138d95d6e1"
down_revision = "d45c986e2727"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("response", sa.Column("plugin", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("response", "plugin")
