// src/sink/pg/flushers/stake_distr.ts
import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

/**
 * Flushes staking distribution events to the database in batches.
 *
 * @param client - PostgreSQL client for executing queries.
 * @param rows - Array of distribution event rows to insert.
 * @returns Promise<void>
 */

export async function flushStakeDistr(client: PoolClient, rowsAll: any[]): Promise<void> {
  if (!rowsAll.length) return;
  await client.query(`SET LOCAL statement_timeout = '30s'`);
  await client.query(`SET LOCAL lock_timeout = '5s'`);

  // ✅ FIX: Filter rows with required fields to prevent NULL constraint errors
  const rows = rowsAll.filter((r) => r && r.delegator_address && r.event_type);
  if (!rows.length) return;

  const cols = [
    'height',
    'tx_hash',
    'msg_index',
    'event_type',
    'delegator_address',
    'validator_address',
    'denom',
    'amount',
    'withdraw_address',
  ];
  await execBatchedInsert(
    client,
    'stake.distribution_events',
    cols,
    rows,
    'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING',
    undefined,
    { maxRows: 5000, maxParams: 30000 },
  );
}
