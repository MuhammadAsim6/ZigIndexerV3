import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

// ✅ Maximum size for a single message JSON (1MB is plenty for most but prevents PG OOM)
const MAX_MSG_VALUE_SIZE = 1_000_000;
const PREVIEW_SIZE = 4096;

function toJsonbSafeString(input: unknown, maxSize: number): string {
    let payload: unknown = input;
    if (typeof input === 'string') {
        try {
            payload = JSON.parse(input);
        } catch {
            payload = {
                _non_json: true,
                _raw: input.length > PREVIEW_SIZE ? input.slice(0, PREVIEW_SIZE) : input,
                _raw_length: input.length,
            };
        }
    }

    let json: string;
    try {
        json = JSON.stringify(payload ?? {});
    } catch {
        return JSON.stringify({ _error: 'failed_to_serialize' });
    }

    if (json.length <= maxSize) return json;

    return JSON.stringify({
        _truncated: true,
        _reason: 'max_size',
        _original_length: json.length,
        _preview: json.slice(0, PREVIEW_SIZE),
    });
}

export async function insertMsgs(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = ['tx_hash', 'msg_index', 'height', 'type_url', 'value', 'signer'];

    // Keep JSONB payload valid; never truncate raw JSON text at arbitrary boundaries.
    const safeRows = rows.map(r => {
        return {
            ...r,
            value: toJsonbSafeString(r.value, MAX_MSG_VALUE_SIZE),
        };
    });

    await execBatchedInsert(
        client,
        'core.messages',
        cols,
        safeRows,
        'ON CONFLICT (height, tx_hash, msg_index) DO NOTHING',
        { value: 'jsonb' },
        { maxRows: 100, maxParams: 600 }
    );
}
