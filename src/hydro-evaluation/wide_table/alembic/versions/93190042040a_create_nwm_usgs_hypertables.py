"""create nwm usgs hypertables

Revision ID: 93190042040a
Revises: a1e08632de83
Create Date: 2022-12-09 10:30:21.745701

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '93190042040a'
down_revision = 'a1e08632de83'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        SELECT create_hypertable(
            'nwm_data',
            'value_time'
        );
    """)
    op.execute("""
        SELECT create_hypertable(
            'usgs_data',
            'value_time'
        );
    """)
    pass

def downgrade() -> None:
    pass
