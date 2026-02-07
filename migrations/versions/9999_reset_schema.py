"""reset schema

Revision ID: 9999_reset_schema
Revises: 45dfca03d756
Create Date: 2026-02-07
"""

from alembic import op
import sqlalchemy as sa

revision = "9999_reset_schema"
down_revision = "45dfca03d756"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    dialect = bind.dialect.name

    if dialect == "postgresql":
        op.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        op.execute("CREATE SCHEMA public;")
        op.execute("GRANT ALL ON SCHEMA public TO public;")
        return

    inspector = sa.inspect(bind)
    tables = inspector.get_table_names()
    tables = [t for t in tables if t != "alembic_version"]

    if dialect == "sqlite":
        op.execute("PRAGMA foreign_keys=OFF;")

    for t in tables:
        op.drop_table(t)

    if dialect == "sqlite":
        op.execute("PRAGMA foreign_keys=ON;")


def downgrade():
    pass
