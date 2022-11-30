"""adds unique const

Revision ID: 58f330a21952
Revises: 679dd138e7bb
Create Date: 2022-11-25 15:16:20.078713

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '58f330a21952'
down_revision = '679dd138e7bb'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint("datetime_timeseries_id_unq", 'values', ['timeseries_id', 'datetime'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("datetime_timeseries_id_unq", 'values', type_='unique')
    # ### end Alembic commands ###