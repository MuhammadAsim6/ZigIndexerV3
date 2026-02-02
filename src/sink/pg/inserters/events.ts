import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

// ✅ Maximum size for individual attribute values (1MB - allows full contract data)
export const MAX_ATTR_VALUE_SIZE = 1_000_000;

// ✅ Maximum total size for the entire serialized attributes JSON (20MB - extensive safety margin)
export const MAX_TOTAL_JSON_SIZE = 20_000_000;

// ✅ Maximum number of attributes to keep (10000 - handles massive batched events)
export const MAX_ATTR_COUNT = 10000;

/**
 * Truncates large attribute values to prevent PostgreSQL index size errors.
 * Some blockchain events (like WASM code uploads) can have 7MB+ attribute values
 * which exceed PostgreSQL's 8KB index row limit.
 */
function sanitizeAttributes(attributes: any[]): any[] {
    if (!Array.isArray(attributes)) return attributes;

    // ✅ CRITICAL: Limit array length to prevent OOM during JSON.stringify
    const limited = attributes.length > MAX_ATTR_COUNT
        ? attributes.slice(0, MAX_ATTR_COUNT)
        : attributes;

    return limited.map((attr: any) => {
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
 * Safely serializes attributes with a total size cap.
 * Handles both array and string inputs. Checks size BEFORE any heavy operations.
 */
export function safeSerializeAttributes(attributes: any): string {
    // ✅ Handle null/undefined
    if (attributes == null) {
        return '[]';
    }

    // ✅ CRITICAL FIX: If already a string, check length FIRST before any operations
    if (typeof attributes === 'string') {
        if (attributes.length > MAX_TOTAL_JSON_SIZE) {
            return attributes.substring(0, MAX_TOTAL_JSON_SIZE - 50) + '...[TRUNCATED_JSON]"]';
        }
        return attributes;
    }

    // ✅ Check array length before any processing
    if (Array.isArray(attributes) && attributes.length > MAX_ATTR_COUNT) {
        // Truncate first, then sanitize
        const truncated = attributes.slice(0, MAX_ATTR_COUNT);
        truncated.push({ key: '_truncated', value: `${attributes.length - MAX_ATTR_COUNT} more items...` });
        const sanitized = sanitizeAttributes(truncated);
        try {
            return JSON.stringify(sanitized);
        } catch {
            return '{"error": "failed_to_serialize"}';
        }
    }

    // If array, sanitize individual values first
    const sanitized = sanitizeAttributes(attributes);

    // Serialize with a try-catch in case of massive nested objects
    let json: string;
    try {
        json = JSON.stringify(sanitized);
    } catch {
        return '{"error": "failed_to_serialize"}';
    }

    if (json.length > MAX_TOTAL_JSON_SIZE) {
        json = json.substring(0, MAX_TOTAL_JSON_SIZE - 50) + '...[TRUNCATED_JSON]"]';
    }

    return json;
}

/**
 * Inserts events into the hash-partitioned core.events table.
 */
export async function insertEvents(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['tx_hash', 'msg_index', 'event_index', 'event_type', 'attributes', 'height'];

    // ✅ SOLID FIX: Sanitize, truncate, and limit total JSON size
    const safeRows = rows.map(r => ({
        ...r,
        attributes: safeSerializeAttributes(r.attributes)
    }));

    // ✅ CRITICAL: Force small batch size to prevent PostgreSQL memory allocation errors
    // ✅ FIX: ON CONFLICT must include `height` since PK is now (height, tx_hash, msg_index, event_index)
    await execBatchedInsert(
        client,
        'core.events',
        cols,
        safeRows,
        'ON CONFLICT (height, tx_hash, msg_index, event_index) DO NOTHING',
        { attributes: 'jsonb' },
        { maxRows: 100, maxParams: 600 }  // 100 rows × 6 cols = 600 params
    );
}
