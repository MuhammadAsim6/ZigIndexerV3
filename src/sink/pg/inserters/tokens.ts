import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

/**
 * Insert or update tokens in the universal registry.
 */
export async function insertTokenRegistry(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'denom',
        'type',
        'base_denom',
        'symbol',
        'decimals',
        'creator',
        'first_seen_height',
        'first_seen_tx',
        'metadata'
    ];

    // ✅ Deduplicate: Keep the last occurrence of each denom in the batch
    // (Last occurrence is likely to have more complete/recent metadata)
    const uniqueRows = Array.from(
        rows.reduce((map, row) => map.set(row.denom, row), new Map()).values()
    );

    await execBatchedInsert(
        client,
        'tokens.registry',
        cols,
        uniqueRows,
        'ON CONFLICT (denom) DO UPDATE SET ' +
        // Allow correcting older misclassified rows (e.g., native -> ibc/cw20/factory).
        'type = CASE ' +
        'WHEN tokens.registry.type = \'native\' AND EXCLUDED.type <> \'native\' THEN EXCLUDED.type ' +
        'ELSE tokens.registry.type END, ' +
        'base_denom = COALESCE(EXCLUDED.base_denom, tokens.registry.base_denom), ' +
        'symbol = COALESCE(EXCLUDED.symbol, tokens.registry.symbol), ' +
        'decimals = COALESCE(EXCLUDED.decimals, tokens.registry.decimals), ' +
        'creator = COALESCE(EXCLUDED.creator, tokens.registry.creator), ' +
        'metadata = tokens.registry.metadata || EXCLUDED.metadata, ' +
        'updated_at = NOW()'
    );
}
