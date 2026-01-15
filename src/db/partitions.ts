import { PoolClient } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('db/partitions');
// Unique Lock ID for Postgres (Hex for 'part') to prevent race conditions
const PARTITION_LOCK_ID = 0x70617274;

/**
 * Ensures partitions exist for the given height range.
 * Uses "Smart Partitioning" logic to create tables only when needed.
 * Also implement 16-way Hash partitioning for core.events.
 */
export async function ensureCorePartitions(client: PoolClient, minHeight: number, maxHeight: number): Promise<void> {
  // Full list of partitioned tables (Range by Height/Sequence)
  const tables = [
    // Core
    ['core', 'blocks'], ['core', 'transactions'], ['core', 'messages'], ['core', 'event_attrs'],
    ['core', 'validator_set'], ['core', 'validator_missed_blocks'], ['core', 'network_params'],

    // Modules
    ['bank', 'transfers'], ['bank', 'balance_deltas'],
    ['stake', 'delegation_events'], ['stake', 'distribution_events'],
    ['gov', 'deposits'], ['gov', 'votes'],
    ['wasm', 'executions'], ['wasm', 'events'], ['wasm', 'event_attrs'], ['wasm', 'state_kv'], ['wasm', 'contract_migrations'],
    ['ibc', 'packets'], // Note: Uses sequence, but handled by height proxy here for simplicity

    // Zigchain
    ['zigchain', 'dex_swaps'], ['zigchain', 'dex_liquidity']
  ];

  // 🔒 Lock: Ensure only one worker checks partitions at a time
  await client.query(`SELECT pg_advisory_lock($1)`, [PARTITION_LOCK_ID]);

  try {
    // 1. Ensure 16 Hash partitions for events
    await ensureEventsHashPartitions(client);

    // 2. Ensure Range partitions for BOTH min and max heights
    for (const [schema, table] of tables) {
      // Ensure partition for minHeight (start of range)
      await client.query(
        `SELECT util.ensure_partition_for_height($1, $2, $3)`,
        [schema, table, minHeight]
      );
      // Ensure partition for maxHeight (end of range)
      // Only if maxHeight is in a different partition bucket
      const rangeSize = 100000; // Default from util.height_part_ranges
      if (Math.floor(minHeight / rangeSize) !== Math.floor(maxHeight / rangeSize)) {
        await client.query(
          `SELECT util.ensure_partition_for_height($1, $2, $3)`,
          [schema, table, maxHeight]
        );
      }
    }
  } catch (err: any) {
    log.error(`Failed to ensure partitions: ${err.message}`);
    throw err;
  } finally {
    // 🔓 Unlock
    await client.query(`SELECT pg_advisory_unlock($1)`, [PARTITION_LOCK_ID]);
  }
}

/**
 * Ensures 64 hash-based partitions exist for the "core.events" table.
 * Using 64 buckets for better distribution at 5.4M+ blocks scale.
 */
async function ensureEventsHashPartitions(client: PoolClient): Promise<void> {
  const modulus = 64; // ✅ Increased from 16 for better scaling
  for (let r = 0; r < modulus; r++) {
    const suffix = r.toString().padStart(2, '0');
    const sql = `
      CREATE TABLE IF NOT EXISTS "core"."events_h${suffix}"
      PARTITION OF "core"."events"
      FOR VALUES WITH (MODULUS ${modulus}, REMAINDER ${r});
    `;
    await client.query(sql);
  }
}
