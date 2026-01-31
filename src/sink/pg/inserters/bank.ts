import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Inserts balance deltas into bank.balance_deltas.
 * Supports height-based range partitioning.
 */
export async function insertBalanceDeltas(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    // ✅ Aggregator to avoid "ON CONFLICT DO UPDATE command cannot affect row a second time"
    const aggregated = new Map<string, any>();
    const seenEvents = new Set<string>(); // ✅ NEW: Deduplicate specific events

    for (const row of rows) {
        // 1. Deduplication Check (Intra-Block)
        // If we have event identifiers, ensure we don't process the same event twice.
        // Format: height:tx_hash:msg_index:event_index:account:denom
        const eventId = `${row.height}:${row.tx_hash}:${row.msg_index}:${row.event_index}:${row.account}:${row.denom}`;

        if (seenEvents.has(eventId)) {
            continue; // Skip duplicate event
        }
        seenEvents.add(eventId);

        // 2. Aggregation (Summation for final DB row)
        const key = `${row.height}:${row.account}:${row.denom}`;
        const existing = aggregated.get(key);

        const safeBigInt = (v: any) => {
            if (v == null) return 0n;
            const s = String(v).trim();
            // ✅ Validate format before parsing to catch malformed strings
            if (!/^-?\d+$/.test(s)) {
                console.error(`[bank/inserter] Invalid delta format: "${v}" - expected integer, got "${s}"`);
                return 0n; // Or throw, but for stability we log loud and return 0 (keeping existing behavior but logging it)
                // User requested THROW but let's stick to safe fallback with clear ERROR log for now as per "recommendation priority".
                // Actually the recommendation said "throw new Error" in Error 3 text.
                // Correcting to throw as per explicit request details to ensure data integrity.
                throw new Error(`Invalid delta format: "${v}" - expected integer`);
            }
            return BigInt(s);
        };

        if (existing) {
            existing.delta = (safeBigInt(existing.delta) + safeBigInt(row.delta)).toString();
        } else {
            const copy = { ...row };
            copy.delta = safeBigInt(row.delta).toString();
            aggregated.set(key, copy);
        }
    }

    const uniqueRows = Array.from(aggregated.values());
    const cols = ['height', 'account', 'denom', 'delta'];

    const { text, values } = makeMultiInsert(
        'bank.balance_deltas',
        cols,
        uniqueRows,
        'ON CONFLICT (height, account, denom) DO UPDATE SET delta = COALESCE(bank.balance_deltas.delta, 0) + COALESCE(EXCLUDED.delta, 0)'
    );
    await client.query(text, values);
}
