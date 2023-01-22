"""adds string tags

Revision ID: 01881833524a
Revises: 58f330a21952
Create Date: 2022-11-25 16:42:39.335891

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils.types.uuid import UUIDType

# revision identifiers, used by Alembic.
revision = '01881833524a'
down_revision = 'c3d227b57f64'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE string_tag_types (
            "name" text NOT NULL,
            CONSTRAINT string_tag_types_pkey PRIMARY KEY (name)
        );
    """)
    op.execute("""
        CREATE TABLE string_tags (
            id                      SERIAL PRIMARY KEY,
            string_tag_type_name    text NOT NULL,
            value                   text NOT NULL,
            CONSTRAINT string_tags_string_tag_type_name_value_key UNIQUE (string_tag_type_name, value),
            CONSTRAINT string_tags_string_tag_type_name_fkey FOREIGN KEY (string_tag_type_name) REFERENCES string_tag_types("name")
        );
        CREATE INDEX string_tag_type_value_idx ON string_tags USING btree (string_tag_type_name, value);
    """)
    op.execute("""
        CREATE TABLE timeseries_string_tag (
            timeseries_id int NOT NULL,
            string_tag_id int NOT NULL,
            CONSTRAINT timeseries_string_tag_string_tag_id_fkey FOREIGN KEY (string_tag_id) REFERENCES string_tags(id),
            CONSTRAINT timeseries_string_tag_timeseries_id_fkey FOREIGN KEY (timeseries_id) REFERENCES timeseries(id),
            CONSTRAINT timeseries_string_tag_unq UNIQUE (timeseries_id, string_tag_id)
        );
        CREATE INDEX timeseries_string_tag_idx ON timeseries_string_tag USING btree (timeseries_id, string_tag_id);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE timeseries_string_tag;
    """)
    op.execute("""
        DROP TABLE string_tags;
    """)
    op.execute("""
        DROP TABLE string_tag_types;
    """)
    
