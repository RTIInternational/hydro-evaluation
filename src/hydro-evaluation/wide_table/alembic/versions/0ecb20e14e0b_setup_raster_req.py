"""setup raster req

Revision ID: 0ecb20e14e0b
Revises: fdae4aa559a6
Create Date: 2023-01-04 17:29:43.786687

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0ecb20e14e0b'
down_revision = 'fdae4aa559a6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER DATABASE postgres SET postgis.enable_outdb_rasters = true;
    """)
    op.execute("""
        ALTER DATABASE postgres SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
    """)
    op.execute("""
        INSERT INTO spatial_ref_sys (srid, proj4text)
        VALUES (
            990000,
            '+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs'
        );
    """)

def downgrade() -> None:
    pass
