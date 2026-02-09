-- initdb/072-reconcile-state.sql
-- Purpose: Persist one-time full reconciliation completion marker.

CREATE TABLE IF NOT EXISTS core.reconcile_state (
    id TEXT PRIMARY KEY,
    full_reconcile_done BOOLEAN NOT NULL DEFAULT false,
    full_reconcile_done_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
