import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  const cols = ['tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes', 'height'];

  // ✅ SOLID FIX: Explicitly stringify JSON to prevent parsing errors
  const safeRows = rows.map(r => ({
    ...r,
    attributes: JSON.stringify(r.attributes)
  }));

  const { text, values } = makeMultiInsert(
    'core.events',
    cols,
    safeRows, // Use safeRows
    'ON CONFLICT (tx_hash, msg_index, event_index) DO NOTHING',
    { attributes: 'jsonb' }
  );
  await client.query(text, values);
}