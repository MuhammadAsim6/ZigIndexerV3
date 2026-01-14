import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmEventAttrs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  const cols = ['contract', 'height', 'tx_hash', 'msg_index', 'event_index', 'key', 'value'];
  const { text, values } = makeMultiInsert(
    'wasm.event_attrs',
    cols,
    rows,
    'ON CONFLICT (height, tx_hash, msg_index, event_index, key) DO NOTHING'
  );
  await client.query(text, values);
}
