"""create nwm_data table

Revision ID: 5700dbf7375b
Revises: 
Create Date: 2022-12-09 10:01:06.476886

Schema based on hydrotools pd.DataFrame:

    0   reference_time    datetime64[ns]
    1   value_time        datetime64[ns]
    2   nwm_feature_id    int64         
    3   value             float32       
    4   usgs_site_code    category      
    5   configuration     category      
    6   measurement_unit  category      
    7   variable_name     category  
"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '5700dbf7375b'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nwm_data (
            reference_time    TIMESTAMPTZ NOT NULL,
            value_time        TIMESTAMPTZ NOT NULL,
            nwm_feature_id    INTEGER,    
            value             float8,
            configuration     text,     
            measurement_unit  text,     
            variable_name     text
        );
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_unique_nwm_data ON nwm_data (
            reference_time,
            value_time,
            nwm_feature_id,    
            configuration,     
            measurement_unit,     
            variable_name
        );
        CREATE INDEX idx_nwm_data_nwm_feature_id ON nwm_data (nwm_feature_id, value_time);
        CREATE INDEX idx_nwm_data_variable_name ON nwm_data (variable_name, value_time);
        CREATE INDEX idx_nwm_data_reference_time ON nwm_data (reference_time, value_time);
        CREATE INDEX idx_nwm_data_configuration ON nwm_data (configuration, value_time);
    """)

def downgrade() -> None:
    op.execute("""
        DROP TABLE nwm_data;
    """)
