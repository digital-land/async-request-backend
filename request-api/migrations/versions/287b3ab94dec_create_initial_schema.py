"""Create initial schema

Revision ID: 287b3ab94dec
Revises:
Create Date: 2024-03-26 13:04:19.879832

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "287b3ab94dec"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "request",
        sa.Column("id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column(
            "created",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        ),
        sa.Column(
            "modified",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "response",
        sa.Column(
            "id", sa.BIGINT(), primary_key=True, autoincrement=True, nullable=False
        ),
        sa.Column("request_id", sa.Text(), sa.ForeignKey("request.id"), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "response_details",
        sa.Column(
            "id", sa.BIGINT(), primary_key=True, autoincrement=True, nullable=False
        ),
        sa.Column(
            "response_id", sa.BIGINT(), sa.ForeignKey("response.id"), nullable=False
        ),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade():
    op.drop_table("response_details")
    op.drop_table("response")
    op.drop_table("request")
