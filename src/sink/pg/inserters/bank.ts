import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts balance deltas into bank.balance_deltas.
 * Supports height-based range partitioning.
 */
export async function insertBalanceDeltas(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['height', 'account', 'denom', 'delta'];
    const { text, values } = makeMultiInsert(
        'bank.balance_deltas',
        cols,
        rows,
        'ON CONFLICT (height, account, denom) DO NOTHING'
    );
    await client.query(text, values);
}
