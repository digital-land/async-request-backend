"""Add Data Submissions without changing existing processing requests.

Revision ID: f3b150a01c01
Revises: d45c986e2727
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "f3b150a01c01"
down_revision = "d45c986e2727"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "add_data_submission",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reference", sa.String(), nullable=False),
        *[
            sa.Column(
                name,
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            )
            for name in ("created", "modified", "submitted_at", "status_updated_at")
        ],
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="TO_BE_ADDED"),
        *[
            sa.Column(name, sa.String())
            for name in (
                "assignee",
                "contact_name",
                "contact_email",
                "organisation_name",
                "geom_type",
                "closed_reason",
                "jira_issue_key",
            )
        ],
        *[
            sa.Column(name, sa.String(), nullable=False)
            for name in (
                "organisation",
                "dataset",
                "collection",
                "endpoint_url",
                "documentation_url",
            )
        ],
        sa.Column(
            "origin_request_id",
            sa.String(),
            sa.ForeignKey("request.id"),
            nullable=False,
        ),
        sa.Column("check_request_id", sa.String(), sa.ForeignKey("request.id")),
        sa.Column("preview_request_id", sa.String(), sa.ForeignKey("request.id")),
        sa.Column(
            "related_add_data_submission_id",
            sa.String(),
            sa.ForeignKey("add_data_submission.id"),
        ),
        sa.Column("add_data_details", JSONB()),
        sa.UniqueConstraint("reference", name="uq_add_data_submission_reference"),
        sa.UniqueConstraint(
            "origin_request_id", "dataset", name="uq_add_data_submission_origin_dataset"
        ),
        sa.CheckConstraint(
            "reference = upper(reference)",
            name="ck_add_data_submission_reference_upper",
        ),
        sa.CheckConstraint(
            "status IN ('TO_BE_ADDED', 'IN_PROGRESS', 'QUERIED', 'ADDED', 'CLOSED')",
            name="ck_add_data_submission_status",
        ),
        sa.CheckConstraint(
            "source IN ('check_tool', 'manage')", name="ck_add_data_submission_source"
        ),
    )
    for name in (
        "status",
        "assignee",
        "check_request_id",
        "preview_request_id",
        "related_add_data_submission_id",
    ):
        op.create_index(f"ix_add_data_submission_{name}", "add_data_submission", [name])
    op.create_index(
        "uq_add_data_submission_active_endpoint",
        "add_data_submission",
        ["endpoint_url", "dataset", "organisation"],
        unique=True,
        postgresql_where=sa.text("status NOT IN ('ADDED', 'CLOSED')"),
    )
    op.add_column(
        "request", sa.Column("add_data_submission_id", sa.String(), nullable=True)
    )
    op.create_foreign_key(
        "fk_request_add_data_submission",
        "request",
        "add_data_submission",
        ["add_data_submission_id"],
        ["id"],
    )
    op.create_index(
        "ix_request_add_data_submission_id", "request", ["add_data_submission_id"]
    )
    op.create_table(
        "add_data_submission_event",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "add_data_submission_id",
            sa.String(),
            sa.ForeignKey("add_data_submission.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("from_status", sa.String()),
        sa.Column("to_status", sa.String(), nullable=False),
        sa.Column("actor", sa.String()),
        sa.Column("assignee", sa.String()),
        sa.Column("note", sa.String()),
        sa.Column(
            "created",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_add_data_submission_event_add_data_submission_id",
        "add_data_submission_event",
        ["add_data_submission_id"],
    )


def downgrade():
    op.drop_table("add_data_submission_event")
    op.drop_index("ix_request_add_data_submission_id", table_name="request")
    op.drop_constraint("fk_request_add_data_submission", "request", type_="foreignkey")
    op.drop_column("request", "add_data_submission_id")
    op.drop_table("add_data_submission")
