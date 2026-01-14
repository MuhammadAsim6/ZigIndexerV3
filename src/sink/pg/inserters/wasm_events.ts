import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes'];

  const safeRows = rows.map(r => ({
    ...r,
    attributes: toJsonSafe(r.attributes)
  }));

  const { text, values } = makeMultiInsert(
    'wasm.events',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index) DO NOTHING',
    { attributes: 'jsonb' }
  );
  await client.query(text, values);
}

function toJsonSafe(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}
