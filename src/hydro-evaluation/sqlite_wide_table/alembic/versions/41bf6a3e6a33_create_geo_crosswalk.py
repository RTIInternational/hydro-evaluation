"""create geo-crosswalk

Revision ID: 41bf6a3e6a33
Revises: 924168a17e91
Create Date: 2022-12-09 12:21:24.121555

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '41bf6a3e6a33'
down_revision = '924168a17e91'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nwm_usgs_xwalk (
            id                SERIAL PRIMARY KEY,
            nwm_feature_id    int NOT NULL,
            usgs_site_code    text NOT NULL
        );
    """)
    op.execute("""
        CREATE INDEX idxnwm_usgs_xwalk_nwm_feature_id ON nwm_usgs_xwalk (nwm_feature_id);
    """)
    op.execute("""
        CREATE INDEX idxnwm_usgs_xwalk_usgs_site_code ON nwm_usgs_xwalk (usgs_site_code);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE nwm_usgs_xwalk;
    """)
