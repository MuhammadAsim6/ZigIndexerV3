import type { PoolClient } from 'pg';
import { insertAttrs } from '../inserters/attrs.js'; // ✅ Import from Inserter

/**
 * Flushes attributes to the DB.
 * Connects the Flush Logic -> Inserter Logic.
 */
export async function flushAttrs(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // Safety timeouts
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);
  
  // ✅ Delegate to the Inserter
  await insertAttrs(client, rows);
}