import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

export async function flushAuthzGrants(client: PoolClient, rowsAll: any[]): Promise<void> {
  if (!rowsAll.length) return;
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  const rows = rowsAll.filter((r) => r && r.granter && r.grantee && r.msg_type_url);
  if (!rows.length) return;

  const columns = ['granter', 'grantee', 'msg_type_url', 'expiration', 'height', 'revoked'] as const;
  const shaped = rows.map((r) => ({
    granter: r.granter,
    grantee: r.grantee,
    msg_type_url: r.msg_type_url,
    expiration: r.expiration ? new Date(r.expiration).toISOString() : null,
    height: r.height,
    revoked: Boolean(r.revoked),
  }));

  await execBatchedInsert(
    client,
    'authz_feegrant.authz_grants',
    columns as unknown as string[],
    shaped,
    'ON CONFLICT DO NOTHING',
  );
}
