"""create postgis ext

Revision ID: 924168a17e91
Revises: 93190042040a
Create Date: 2022-12-09 12:11:44.854551

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '924168a17e91'
down_revision = '93190042040a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS postgis_raster;
    """)
    pass


def downgrade() -> None:
    # op.execute("""
    #     DROP EXTENSION postgis;
    # """)
    pass
