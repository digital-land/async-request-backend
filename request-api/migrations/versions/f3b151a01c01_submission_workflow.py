"""Add workflow concurrency version and closure event details.

Revision ID: f3b151a01c01
Revises: f3b150a01c01
"""

from alembic import op
import sqlalchemy as sa

revision = "f3b151a01c01"
down_revision = "f3b150a01c01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "add_data_submission",
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column("add_data_submission_event", sa.Column("closed_reason", sa.String()))
    op.add_column(
        "add_data_submission_event",
        sa.Column("related_add_data_submission_id", sa.String()),
    )


def downgrade():
    op.drop_column("add_data_submission_event", "related_add_data_submission_id")
    op.drop_column("add_data_submission_event", "closed_reason")
    op.drop_column("add_data_submission", "version")
