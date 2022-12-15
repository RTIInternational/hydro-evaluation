"""adds timeseries and values

Revision ID: 5cf6db1771fd
Revises: 
Create Date: 2022-11-24 09:49:00.953348

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '5cf6db1771fd'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE timeseries (
            id          SERIAL PRIMARY KEY,
            "name"      text NOT NULL
        );
    """)
    op.execute("""
        CREATE TABLE "values" (
            datetime        timestamp NOT NULL,
            value           float8 NOT NULL,
            timeseries_id   int NOT NULL,
            CONSTRAINT datetime_timeseries_id_unq UNIQUE (timeseries_id, datetime),
            CONSTRAINT values_timeseries_id_fkey FOREIGN KEY (timeseries_id) REFERENCES timeseries(id)
        );
        CREATE INDEX values_datetime_idx ON "values" USING btree (datetime DESC);
        CREATE INDEX values_timeseries_id_idx ON "values" USING btree (timeseries_id);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE "values";
    """)
    op.execute("""
        DROP TABLE timeseries;
    """)
