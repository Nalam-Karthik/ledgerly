"""initial schema

Revision ID: 617ca90e50e0
Revises: 
Create Date: 2026-11-13 06:24:25.315732
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '617ca90e50e0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('outbox_events',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('event_type', sa.String(length=64), nullable=False),
    sa.Column('aggregate_id', sa.UUID(), nullable=False),
    sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_outbox_unpublished', 'outbox_events', ['created_at'], unique=False, postgresql_where=sa.text('published_at IS NULL'))
    op.create_table('users',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('email', sa.String(length=320), nullable=False),
    sa.Column('password_hash', sa.String(length=255), nullable=False),
    sa.Column('role', sa.Enum('user', 'admin', name='role'), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('email')
    )
    op.create_table('accounts',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('owner_id', sa.UUID(), nullable=True),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('balance', sa.BigInteger(), server_default='0', nullable=False),
    sa.Column('is_system', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('char_length(currency) = 3', name='iso_currency'),
    sa.CheckConstraint('is_system OR balance >= 0', name='non_negative_balance'),
    sa.ForeignKeyConstraint(['owner_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_accounts_owner_id'), 'accounts', ['owner_id'], unique=False)
    op.create_index('uq_system_account_currency', 'accounts', ['currency'], unique=True, postgresql_where=sa.text('is_system'))
    op.create_table('idempotency_keys',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('key', sa.String(length=255), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('status_code', sa.Integer(), nullable=False),
    sa.Column('response_body', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('user_id', 'key')
    )
    op.create_table('refresh_tokens',
    sa.Column('jti', sa.UUID(), nullable=False),
    sa.Column('family_id', sa.UUID(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('jti')
    )
    op.create_index(op.f('ix_refresh_tokens_family_id'), 'refresh_tokens', ['family_id'], unique=False)
    op.create_table('transactions',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('kind', sa.Enum('deposit', 'withdrawal', 'transfer', name='txn_kind'), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('currency', sa.String(length=3), nullable=False),
    sa.Column('description', sa.String(length=255), nullable=True),
    sa.Column('created_by', sa.UUID(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('amount > 0', name='positive_amount'),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('webhook_endpoints',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('url', sa.Text(), nullable=False),
    sa.Column('secret', sa.String(length=64), nullable=False),
    sa.Column('active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_webhook_endpoints_user_id'), 'webhook_endpoints', ['user_id'], unique=False)
    op.create_table('ledger_entries',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('transaction_id', sa.UUID(), nullable=False),
    sa.Column('account_id', sa.UUID(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('balance_after', sa.BigInteger(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['account_id'], ['accounts.id'], ),
    sa.ForeignKeyConstraint(['transaction_id'], ['transactions.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_ledger_entries_account_id_id', 'ledger_entries', ['account_id', 'id'], unique=False)
    op.create_index(op.f('ix_ledger_entries_transaction_id'), 'ledger_entries', ['transaction_id'], unique=False)
    op.create_table('webhook_deliveries',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('event_id', sa.UUID(), nullable=False),
    sa.Column('endpoint_id', sa.UUID(), nullable=False),
    sa.Column('status', sa.Enum('pending', 'delivered', 'failed', name='delivery_status'), nullable=False),
    sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
    sa.Column('last_error', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['endpoint_id'], ['webhook_endpoints.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('event_id', 'endpoint_id', name='uq_delivery_event_endpoint')
    )


def downgrade() -> None:
    op.drop_table('webhook_deliveries')
    op.drop_index(op.f('ix_ledger_entries_transaction_id'), table_name='ledger_entries')
    op.drop_index('ix_ledger_entries_account_id_id', table_name='ledger_entries')
    op.drop_table('ledger_entries')
    op.drop_index(op.f('ix_webhook_endpoints_user_id'), table_name='webhook_endpoints')
    op.drop_table('webhook_endpoints')
    op.drop_table('transactions')
    op.drop_index(op.f('ix_refresh_tokens_family_id'), table_name='refresh_tokens')
    op.drop_table('refresh_tokens')
    op.drop_table('idempotency_keys')
    op.drop_index('uq_system_account_currency', table_name='accounts', postgresql_where=sa.text('is_system'))
    op.drop_index(op.f('ix_accounts_owner_id'), table_name='accounts')
    op.drop_table('accounts')
    op.drop_table('users')
    op.drop_index('ix_outbox_unpublished', table_name='outbox_events', postgresql_where=sa.text('published_at IS NULL'))
    op.drop_table('outbox_events')
    for enum_name in ("delivery_status", "txn_kind", "role"):
        sa.Enum(name=enum_name).drop(op.get_bind(), checkfirst=True)
