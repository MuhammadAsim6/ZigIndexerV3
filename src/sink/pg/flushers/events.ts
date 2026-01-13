import type { PoolClient } from 'pg';
import { insertEvents } from '../inserters/events.js'; // ✅ Import from Inserter

/**
 * Flushes events to the DB.
 * Connects the Flush Logic -> Inserter Logic.
 */
export async function flushEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // Safety timeouts
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);
  
  // ✅ Delegate to the Inserter
  await insertEvents(client, rows);
}