"""create geo-crosswalk

Revision ID: 41bf6a3e6a33
Revises: 924168a17e91
Create Date: 2022-12-09 12:21:24.121555

"""
import sqlalchemy as sa

from alembic import op

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
            usgs_site_code    text NOT NULL,
            geom              GEOMETRY(Point, 4326) NOT NULL
        );
        CREATE INDEX idxnwm_usgs_xwalk_geom ON nwm_usgs_xwalk USING GIST (geom);
        CREATE INDEX idxnwm_usgs_xwalk_nwm_feature_id ON nwm_usgs_xwalk (nwm_feature_id);
        CREATE INDEX idxnwm_usgs_xwalk_usgs_site_code ON nwm_usgs_xwalk (usgs_site_code);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE nwm_usgs_xwalk;
    """)
