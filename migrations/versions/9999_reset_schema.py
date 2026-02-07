"""reset schema

Revision ID: 9999_reset_schema
Revises: 9dcf82d03262
Create Date: 2026-02-07
"""
from alembic import op

revision = "9999_reset_schema"
down_revision = "9dcf82d03262"
branch_labels = None
depends_on = None


def upgrade():
    # drop semua tabel public (kecuali alembic_version kalau mau)
    op.execute("DROP SCHEMA public CASCADE;")
    op.execute("CREATE SCHEMA public;")
    op.execute("GRANT ALL ON SCHEMA public TO postgres;")
    op.execute("GRANT ALL ON SCHEMA public TO public;")


def downgrade():
    # downgrade tidak disupport untuk reset
    pass
