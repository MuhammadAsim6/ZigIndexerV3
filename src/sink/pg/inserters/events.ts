import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

// ✅ Maximum size for individual attribute values (4KB safe for indexing)
const MAX_ATTR_VALUE_SIZE = 4000;

/**
 * Truncates large attribute values to prevent PostgreSQL index size errors.
 * Some blockchain events (like WASM code uploads) can have 7MB+ attribute values
 * which exceed PostgreSQL's 8KB index row limit.
 */
function sanitizeAttributes(attributes: any[]): any[] {
    if (!Array.isArray(attributes)) return attributes;

    return attributes.map((attr: any) => {
        if (typeof attr?.value === 'string' && attr.value.length > MAX_ATTR_VALUE_SIZE) {
            return {
                key: attr.key,
                value: attr.value.substring(0, MAX_ATTR_VALUE_SIZE) + '...[TRUNCATED]'
            };
        }
        return attr;
    });
}

/**
 * Inserts events into the hash-partitioned core.events table.
 */
export async function insertEvents(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes', 'height'];

    // ✅ SOLID FIX: Sanitize and truncate large attribute values
    const safeRows = rows.map(r => ({
        ...r,
        attributes: JSON.stringify(sanitizeAttributes(r.attributes))
    }));

    const { text, values } = makeMultiInsert(
        'core.events',
        cols,
        safeRows,
        'ON CONFLICT (tx_hash, msg_index, event_index) DO NOTHING',
        { attributes: 'jsonb' }
    );
    await client.query(text, values);
}

