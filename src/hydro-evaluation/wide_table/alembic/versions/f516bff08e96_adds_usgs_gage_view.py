"""adds usgs gage view

Revision ID: f516bff08e96
Revises: 56a938c6c3b3
Create Date: 2022-12-31 11:10:47.185246

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'f516bff08e96'
down_revision = '56a938c6c3b3'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.execute("""
        CREATE OR REPLACE VIEW usgs_gages AS (
            SELECT * FROM route_links rl WHERE rl.gage_id IS NOT NULL AND rl.gage_id ~ '^[0-9\.]+$'
        ); 
    """)


def downgrade() -> None:
    op.execute("""
        DROP VIEW usgs_gages;
    """)
