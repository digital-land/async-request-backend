"""Track the latest successful request publication to S3.

Revision ID: f3b152a01c01
Revises: f3b151a01c01
"""

from alembic import op
import sqlalchemy as sa

revision = "f3b152a01c01"
down_revision = "f3b151a01c01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request", sa.Column("published_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column("request", "published_at")
