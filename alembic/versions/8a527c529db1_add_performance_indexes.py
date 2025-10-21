"""Add performance indexes

Revision ID: 8a527c529db1
Revises: a3a38d9091b1
Create Date: 2025-10-19 19:42:33.921788

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a527c529db1'
down_revision: Union[str, Sequence[str], None] = 'a3a38d9091b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Add composite indexes for query performance
    op.create_index('idx_occurred_user', 'events', ['occurred_at', 'user_id'], if_not_exists = True)
    op.create_index('idx_occurred_type', 'events', ['occurred_at', 'event_type'], if_not_exists = True)



def downgrade():
    op.drop_index('idx_occurred_user', 'events')
    op.drop_index('idx_occurred_type', 'events')
    if_not_exists = True

