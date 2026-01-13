import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'event_type', 'attributes'];
  
  // ✅ SOLID FIX
  const safeRows = rows.map(r => ({
    ...r,
    attributes: JSON.stringify(r.attributes)
  }));

  const { text, values } = makeMultiInsert(
    'wasm.events',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index, event_type) DO NOTHING',
    { attributes: 'jsonb' }
  );
  await client.query(text, values);
}