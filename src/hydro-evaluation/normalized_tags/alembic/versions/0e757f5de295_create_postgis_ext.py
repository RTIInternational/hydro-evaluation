"""create postgis ext

Revision ID: 0e757f5de295
Revises: 36ef5b230d7b
Create Date: 2022-12-14 09:27:22.634094

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0e757f5de295'
down_revision = 'f7efb2f6d56c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE EXTENSION postgis;
    """)


def downgrade() -> None:
    op.execute("""
        DROP EXTENSION postgis;
    """)
