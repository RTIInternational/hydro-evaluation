"""create nwm usgs hypertables

Revision ID: 93190042040a
Revises: a1e08632de83
Create Date: 2022-12-09 10:30:21.745701

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '93190042040a'
down_revision = 'a1e08632de83'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        SELECT create_hypertable(
            'nwm_data',
            'value_time',
            'nwm_feature_id',
            4
        );
    """)
    op.execute("""
        SELECT create_hypertable(
            'usgs_data',
            'value_time',
            'usgs_site_code',
            4
        );
    """)


def downgrade() -> None:
    pass
