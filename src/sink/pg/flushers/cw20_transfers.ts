import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export async function flushCw20Transfers(client: PoolClient, rowsAll: any[]): Promise<void> {
  if (!rowsAll.length) return;
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  const rows = rowsAll.filter((r) => r && r.contract && r.from_addr && r.to_addr && r.amount);
  if (!rows.length) return;

  const columns = ['contract', 'from_addr', 'to_addr', 'amount', 'height', 'tx_hash'] as const;
  const shaped = rows.map((r) => ({
    contract: r.contract,
    from_addr: r.from_addr,
    to_addr: r.to_addr,
    amount: r.amount,
    height: r.height,
    tx_hash: r.tx_hash,
  }));

  await execBatchedInsert(
    client,
    'tokens.cw20_transfers',
    columns as unknown as string[],
    shaped,
    'ON CONFLICT DO NOTHING',
  );
}
