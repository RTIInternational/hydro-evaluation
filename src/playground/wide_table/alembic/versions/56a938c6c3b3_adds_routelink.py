"""adds routelink

Revision ID: 56a938c6c3b3
Revises: 41bf6a3e6a33
Create Date: 2022-12-29 09:09:13.181056

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '56a938c6c3b3'
down_revision = '41bf6a3e6a33'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE route_links (
            id              SERIAL PRIMARY KEY,
            nwm_feature_id  int NOT NULL,
            to_node         int NOT NULL,
            from_node       int NOT NULL,
            gage_id         text,
            geom            GEOMETRY(Point, 4326) NOT NULL
        );
        CREATE INDEX idx_route_link_geom ON route_links USING GIST (geom);
        CREATE INDEX idx_route_link_nwm_feature_id ON route_links (nwm_feature_id);
        CREATE INDEX idx_route_link_gage_id ON route_links (gage_id);
        CREATE INDEX idx_route_link_to_node ON route_links (to_node);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE route_links;
    """)