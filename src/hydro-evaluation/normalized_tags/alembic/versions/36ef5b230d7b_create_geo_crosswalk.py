"""create geo crosswalk

Revision ID: 36ef5b230d7b
Revises: c3d227b57f64
Create Date: 2022-12-14 09:25:12.996367

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '36ef5b230d7b'
down_revision = '0e757f5de295'
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
