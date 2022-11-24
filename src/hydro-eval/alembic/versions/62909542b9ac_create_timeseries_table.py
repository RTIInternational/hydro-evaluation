"""create timeseries table

Revision ID: 62909542b9ac
Revises: 
Create Date: 2022-11-23 19:57:02.895633

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '62909542b9ac'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'timeseries',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(50), nullable=False),
        sa.Column('description', sa.Unicode(200)),
    )


def downgrade() -> None:
    pass
