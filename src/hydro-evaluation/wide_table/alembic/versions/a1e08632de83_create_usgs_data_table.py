"""create usgs_data_table

Revision ID: a1e08632de83
Revises: 5700dbf7375b
Create Date: 2022-12-09 10:19:20.500567

Schema based on hydrotools pd.DataFrame:

0   value_time        datetime64[ns]
1   variable_name     category      
2   usgs_site_code    category      
3   measurement_unit  category      
4   value             float32       
5   qualifiers        category      
6   series            category 
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1e08632de83'
down_revision = '5700dbf7375b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE usgs_data (
            value_time        TIMESTAMPTZ NOT NULL,
            variable_name     text,
            usgs_site_code    text,
            measurement_unit  text,
            value             float8
        );
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_unique_usgs_data ON usgs_data (
            value_time,
            variable_name,
            usgs_site_code,
            measurement_unit
        );
        CREATE INDEX idx_usgs_data_usgs_site_code ON usgs_data (usgs_site_code, value_time);
        CREATE INDEX idx_usgs_data_variable_name ON usgs_data (variable_name, value_time);
    """)

def downgrade() -> None:
    op.execute("""
        DROP TABLE usgs_data;
    """)
