"""create indexes for foreign keys

Revision ID: d45c986e2727
Revises: 287b3ab94dec
Create Date: 2024-05-16 07:38:06.028489

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "d45c986e2727"
down_revision = "287b3ab94dec"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "idx_response_request_id",
        "response",
        ["request_id"],
        unique=False,
    )
    op.create_index(
        "idx_response_details_response_id",
        "response_details",
        ["response_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("idx_response_request_id", table_name="response")
    op.drop_index("idx_response_details_response_id", table_name="response_details")
