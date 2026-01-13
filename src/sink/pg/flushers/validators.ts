// src/sink/pg/flushers/validators.ts
import { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

/**
 * Upsert validator metadata in batches.
 * 
 * @param client - Postgres client.
 * @param rows - Validator metadata rows.
 */
export async function upsertValidators(client: PoolClient, rows: any[]) {
    if (!rows.length) return;

    // 🛡️ PRE-MERGE: Prevent "ON CONFLICT DO UPDATE command cannot affect row a second time"
    const mergedMap = new Map<string, any>();
    for (const row of rows) {
        mergedMap.set(row.operator_address, row); // Keep latest
    }
    const finalRows = Array.from(mergedMap.values());

    const cols = [
        'operator_address', 'moniker', 'website', 'details',
        'commission_rate', 'max_commission_rate', 'max_change_rate',
        'min_self_delegation', 'status', 'updated_at_height', 'updated_at_time'
    ];

    await execBatchedInsert(
        client,
        'core.validators',
        cols,
        finalRows,
        `ON CONFLICT (operator_address) DO UPDATE SET
      moniker = COALESCE(EXCLUDED.moniker, core.validators.moniker),
      website = COALESCE(EXCLUDED.website, core.validators.website),
      details = COALESCE(EXCLUDED.details, core.validators.details),
      commission_rate = COALESCE(EXCLUDED.commission_rate, core.validators.commission_rate),
      status = COALESCE(EXCLUDED.status, core.validators.status),
      updated_at_height = EXCLUDED.updated_at_height,
      updated_at_time = EXCLUDED.updated_at_time`
    );
}
