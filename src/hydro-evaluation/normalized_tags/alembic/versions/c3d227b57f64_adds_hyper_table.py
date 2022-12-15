"""adds hyper table

Revision ID: c3d227b57f64
Revises: 1ea32ca7aa4f
Create Date: 2022-12-13 22:29:59.463224

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c3d227b57f64'
down_revision = '5cf6db1771fd'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        SELECT create_hypertable(
            'values',
            'datetime'
        );
    """)


def downgrade() -> None:
    pass
