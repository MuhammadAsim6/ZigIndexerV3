import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts flattened event attributes.
 * Critical: Includes 'height' for Range Partitioning.
 */
export async function insertAttrs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // ✅ 'height' is mandatory for range partitioning
  const cols = ['tx_hash', 'msg_index', 'event_index', 'key', 'value', 'height'];

  const { text, values } = makeMultiInsert(
    'core.event_attrs',
    cols,
    rows,
    // ✅ Conflict must match PK (height, tx_hash, msg_index, event_index, key)
    'ON CONFLICT (height, tx_hash, msg_index, event_index, key) DO NOTHING'
  );
  await client.query(text, values);
}
