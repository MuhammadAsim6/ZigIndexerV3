import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts IBC Packets safely (converting BigInts to Strings).
 * Includes 'sequence' as TEXT to prevent numeric overflow crashes.
 */
export async function insertIbcPackets(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;

  // 🛡️ PRE-MERGE: Prevent "ON CONFLICT DO UPDATE command cannot affect row a second time"
  // If the same packet has multiple updates in one batch, merge them in-memory.
  const mergedMap = new Map<string, any>();
  for (const row of rows) {
    const key = `${row.channel_id_src}:${row.port_id_src}:${row.sequence}`;
    const existing = mergedMap.get(key);
    if (!existing) {
      mergedMap.set(key, { ...row });
    } else {
      // Merge logic: keep non-null fields, prefer latest status
      if (row.status) existing.status = row.status;
      if (row.tx_hash_send) existing.tx_hash_send = row.tx_hash_send;
      if (row.height_send) existing.height_send = row.height_send;
      if (row.tx_hash_recv) existing.tx_hash_recv = row.tx_hash_recv;
      if (row.height_recv) existing.height_recv = row.height_recv;
      if (row.tx_hash_ack) existing.tx_hash_ack = row.tx_hash_ack;
      if (row.height_ack) existing.height_ack = row.height_ack;
      if (row.relayer) existing.relayer = row.relayer;
      if (row.denom) existing.denom = row.denom;
      if (row.amount) existing.amount = row.amount;
      if (row.memo) existing.memo = row.memo;
    }
  }
  const finalRows = Array.from(mergedMap.values());

  const cols = [
    'port_id_src', 'channel_id_src', 'sequence', 'port_id_dst', 'channel_id_dst',
    'timeout_height', 'timeout_ts', 'status',
    'tx_hash_send', 'height_send',
    'tx_hash_recv', 'height_recv',
    'tx_hash_ack', 'height_ack',
    'relayer', 'denom', 'amount', 'memo'
  ];

  // Note: IBC Packets are partitioned by SEQUENCE, not HEIGHT in our schema.
  // But we store heights as text columns (height_send, etc.)
  const { text, values } = makeMultiInsert(
    'ibc.packets',
    cols,
    finalRows,
    `ON CONFLICT (channel_id_src, port_id_src, sequence) DO UPDATE SET
       status = EXCLUDED.status,
       tx_hash_recv = COALESCE(ibc.packets.tx_hash_recv, EXCLUDED.tx_hash_recv),
       height_recv = COALESCE(ibc.packets.height_recv, EXCLUDED.height_recv),
       tx_hash_ack = COALESCE(ibc.packets.tx_hash_ack, EXCLUDED.tx_hash_ack),
       height_ack = COALESCE(ibc.packets.height_ack, EXCLUDED.height_ack),
       relayer = COALESCE(ibc.packets.relayer, EXCLUDED.relayer)
    `
  );
  await client.query(text, values);
}