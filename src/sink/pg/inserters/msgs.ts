import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertMsgs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  const cols = ['tx_hash', 'msg_index', 'height', 'type_url', 'value', 'signer'];

  // ✅ SOLID FIX: Sanitize Message Value
  const safeRows = rows.map(r => ({
    ...r,
    value: JSON.stringify(r.value)
  }));

  const { text, values } = makeMultiInsert(
    'core.messages',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING',
    { value: 'jsonb' }
  );
  await client.query(text, values);
}