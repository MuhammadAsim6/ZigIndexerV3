import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmExec(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  
  const cols = ['tx_hash', 'msg_index', 'contract', 'caller', 'funds', 'msg', 'success', 'error', 'gas_used', 'height'];
  
  const safeRows = rows.map(r => ({
    ...r,
    funds: toJsonSafe(r.funds),
    msg: toJsonSafe(r.msg ?? {})
  }));

  const { text, values } = makeMultiInsert(
    'wasm.executions',
    cols,
    safeRows,
    'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING',
    { funds: 'jsonb', msg: 'jsonb' }
  );
  await client.query(text, values);
}

function toJsonSafe(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}
