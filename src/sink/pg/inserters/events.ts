import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts events into the hash-partitioned core.events table.
 */
export async function insertEvents(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes', 'height'];

    // ✅ SOLID FIX: Sanitize Attributes JSON
    const safeRows = rows.map(r => ({
        ...r,
        attributes: JSON.stringify(r.attributes)
    }));

    const { text, values } = makeMultiInsert(
        'core.events',
        cols,
        safeRows,
        'ON CONFLICT (tx_hash, msg_index, event_index) DO NOTHING',
        { attributes: 'jsonb' }
    );
    await client.query(text, values);
}
