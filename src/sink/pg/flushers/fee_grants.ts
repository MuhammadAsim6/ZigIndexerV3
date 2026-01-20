import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export async function flushFeeGrants(client: PoolClient, rowsAll: any[]): Promise<void> {
  if (!rowsAll.length) return;
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  const rows = rowsAll.filter((r) => r && r.granter && r.grantee);
  if (!rows.length) return;

  const columns = ['granter', 'grantee', 'allowance', 'expiration', 'height', 'revoked'] as const;
  const shaped = rows.map((r) => ({
    granter: r.granter,
    grantee: r.grantee,
    allowance: r.allowance ?? null,
    expiration: r.expiration ? new Date(r.expiration).toISOString() : null,
    height: r.height,
    revoked: Boolean(r.revoked),
  }));

  await execBatchedInsert(
    client,
    'authz_feegrant.fee_grants',
    columns as unknown as string[],
    shaped,
    'ON CONFLICT DO NOTHING',
    { allowance: 'jsonb' },
  );
}
