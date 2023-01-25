"""adds routing funcs

Revision ID: fdae4aa559a6
Revises: f516bff08e96
Create Date: 2022-12-31 11:14:53.826115

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'fdae4aa559a6'
down_revision = 'f516bff08e96'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE OR REPLACE FUNCTION get_upstream(_nwm_feature_id int)
        RETURNS TABLE (
            nwm_feature_id  int,
            to_node         int,
            geom            GEOMETRY(Point, 4326)
        )
        LANGUAGE plpgsql AS
        $$
        BEGIN
        RETURN QUERY
        WITH RECURSIVE search_tree AS (
                SELECT rl.nwm_feature_id, rl.to_node, rl.geom
                FROM route_links rl
                where rl.nwm_feature_id = _nwm_feature_id
            UNION ALL
                SELECT rl.nwm_feature_id, rl.to_node, rl.geom
                FROM route_links rl, search_tree st
                WHERE rl.to_node = st.nwm_feature_id
            )
            SELECT * FROM search_tree;
        END
        $$; 
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION get_downstream(_nwm_feature_id int)
        RETURNS TABLE (
            nwm_feature_id  int,
            to_node         int,
            geom            GEOMETRY(Point, 4326)
        )
        LANGUAGE plpgsql AS
        $$
        BEGIN
        RETURN QUERY
        WITH RECURSIVE search_tree AS (
                SELECT rl.nwm_feature_id, rl.to_node, rl.geom
                FROM route_links rl
                where rl.nwm_feature_id = _nwm_feature_id
            UNION ALL
                SELECT rl.nwm_feature_id, rl.to_node, rl.geom
                FROM route_links rl, search_tree st
                WHERE rl.nwm_feature_id = st.to_node
            )
            SELECT * FROM search_tree;
        END
        $$; 
    """)


def downgrade() -> None:
    op.execute("""
        DROP FUNCTION get_upstream(_nwm_feature_id int);
    """)
    op.execute("""
        DROP FUNCTION get_downstream(_nwm_feature_id int);
    """)
