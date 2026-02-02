import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';
import { MAX_ATTR_VALUE_SIZE } from './events.js';

/**
 * Inserts flattened event attributes.
 * Critical: Includes 'height' for Range Partitioning.
 */
export async function insertAttrs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  const cols = ['tx_hash', 'msg_index', 'event_index', 'attr_index', 'key', 'value', 'height'];

  // ✅ Truncate large attribute values
  const safeRows = rows.map(r => ({
    ...r,
    value: typeof r.value === 'string' && r.value.length > MAX_ATTR_VALUE_SIZE
      ? r.value.substring(0, MAX_ATTR_VALUE_SIZE) + '...[TRUNCATED]'
      : r.value
  }));

  await execBatchedInsert(
    client,
    'core.event_attrs',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index, attr_index) DO NOTHING',
    {},
    { maxRows: 500 }
  );
}
