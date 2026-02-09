import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('sink/pg/inserters/bank');

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
                // ✅ FIX: Throw error for invalid delta formats to prevent silent data corruption
                throw new Error(`[bank/inserter] Invalid delta format: "${v}" - expected integer, got "${s}"`);
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
        // Idempotency guarantee: if the same (height, account, denom) is replayed,
        // keep the first computed delta and skip duplicates.
        'ON CONFLICT (height, account, denom) DO NOTHING'
    );
    const res = await client.query(text, values);

    const inserted = Number(res.rowCount ?? 0);
    const conflicts = Math.max(0, uniqueRows.length - inserted);
    if (conflicts > 0) {
        log.warn(
            `[bank] idempotent conflict skip: input=${rows.length} aggregated=${uniqueRows.length} inserted=${inserted} skipped=${conflicts}`,
        );
    } else {
        log.debug(`[bank] inserted balance deltas: input=${rows.length} aggregated=${uniqueRows.length} inserted=${inserted}`);
    }
}
