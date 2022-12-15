"""adds datetime tags

Revision ID: f7efb2f6d56c
Revises: 01881833524a
Create Date: 2022-11-27 16:30:37.980552

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils.types.uuid import UUIDType

# revision identifiers, used by Alembic.
revision = 'f7efb2f6d56c'
down_revision = '01881833524a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE datetime_tag_types (
            "name" varchar NOT NULL,
            CONSTRAINT datetime_tag_types_pkey PRIMARY KEY (name)
        );
    """)
    op.execute("""
        CREATE TABLE datetime_tags (
            id                      SERIAL PRIMARY KEY,
            datetime_tag_type_name  text NOT NULL,
            value                   timestamp NOT NULL,
            CONSTRAINT datetime_tags_datetime_tag_type_name_value_key UNIQUE (datetime_tag_type_name, value),
            CONSTRAINT datetime_tags_datetime_tag_type_name_fkey FOREIGN KEY (datetime_tag_type_name) REFERENCES datetime_tag_types("name")
        );
        CREATE INDEX datetime_tag_type_value_idx ON datetime_tags USING btree (datetime_tag_type_name, value);
    """)
    op.execute("""
        CREATE TABLE timeseries_datetime_tag (
            timeseries_id   int NOT NULL,
            datetime_tag_id int NOT NULL,
            CONSTRAINT timeseries_datetime_tag_datetime_tag_id_fkey FOREIGN KEY (datetime_tag_id) REFERENCES datetime_tags(id),
            CONSTRAINT timeseries_datetime_tag_timeseries_id_fkey FOREIGN KEY (timeseries_id) REFERENCES timeseries(id),
            CONSTRAINT timeseries_datetime_tag_unq UNIQUE (timeseries_id, datetime_tag_id)
        );
        CREATE INDEX timeseries_datetime_tag_idx ON timeseries_datetime_tag USING btree (timeseries_id, datetime_tag_id);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE timeseries_datetime_tag;
    """)
    op.execute("""
        DROP TABLE datetime_tags;
    """)
    op.execute("""
        DROP TABLE datetime_tag_types;
    """)
    
