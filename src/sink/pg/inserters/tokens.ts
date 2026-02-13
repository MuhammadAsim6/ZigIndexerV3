import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

type RegistryRow = {
    denom: string;
    type: 'native' | 'factory' | 'cw20' | 'ibc';
    base_denom: string | null;
    symbol: string | null;
    decimals: number | null;
    creator: string | null;
    first_seen_height: number | null;
    first_seen_tx: string | null;
    metadata: Record<string, unknown> | null;
};

function normalizeNonEmptyString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function normalizeHeight(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isSafeInteger(n) || n < 0) return null;
    return n;
}

function normalizeDecimals(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0 || n > 2147483647) return null;
    return n;
}

function normalizeMetadata(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    const obj = value as Record<string, unknown>;
    return Object.keys(obj).length > 0 ? obj : null;
}

function normalizeType(value: unknown): RegistryRow['type'] {
    const type = String(value ?? '').trim();
    if (type === 'factory' || type === 'cw20' || type === 'ibc' || type === 'native') {
        return type;
    }
    return 'native';
}

function chooseType(current: RegistryRow['type'], incoming: RegistryRow['type']): RegistryRow['type'] {
    if (current === 'native' && incoming !== 'native') return incoming;
    return current;
}

function preferIncomingNonEmpty(current: string | null, incoming: string | null): string | null {
    return incoming ?? current;
}

function mergeMetadata(
    current: Record<string, unknown> | null,
    incoming: Record<string, unknown> | null,
): Record<string, unknown> | null {
    if (!current && !incoming) return null;
    return { ...(current ?? {}), ...(incoming ?? {}) };
}

function mergeFirstSeen(
    currentHeight: number | null,
    currentTx: string | null,
    incomingHeight: number | null,
    incomingTx: string | null,
): { height: number | null; tx: string | null } {
    if (currentHeight === null && incomingHeight === null) {
        return { height: null, tx: currentTx ?? incomingTx };
    }
    if (currentHeight === null) {
        return { height: incomingHeight, tx: incomingTx ?? currentTx };
    }
    if (incomingHeight === null) {
        return { height: currentHeight, tx: currentTx ?? incomingTx };
    }
    if (incomingHeight < currentHeight) {
        return { height: incomingHeight, tx: incomingTx ?? currentTx };
    }
    if (incomingHeight === currentHeight) {
        return { height: currentHeight, tx: currentTx ?? incomingTx };
    }
    return { height: currentHeight, tx: currentTx };
}

function sanitizeRow(row: any): RegistryRow | null {
    const denom = normalizeNonEmptyString(row?.denom);
    if (!denom) return null;

    return {
        denom,
        type: normalizeType(row?.type),
        base_denom: normalizeNonEmptyString(row?.base_denom),
        symbol: normalizeNonEmptyString(row?.symbol),
        decimals: normalizeDecimals(row?.decimals),
        creator: normalizeNonEmptyString(row?.creator),
        first_seen_height: normalizeHeight(row?.first_seen_height),
        first_seen_tx: normalizeNonEmptyString(row?.first_seen_tx),
        metadata: normalizeMetadata(row?.metadata),
    };
}

/**
 * Insert or update tokens in the universal registry.
 */
export async function insertTokenRegistry(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'denom',
        'type',
        'base_denom',
        'symbol',
        'decimals',
        'creator',
        'first_seen_height',
        'first_seen_tx',
        'metadata'
    ];

    const mergedByDenom = new Map<string, RegistryRow>();
    for (const rawRow of rows) {
        const incoming = sanitizeRow(rawRow);
        if (!incoming) continue;
        const existing = mergedByDenom.get(incoming.denom);
        if (!existing) {
            mergedByDenom.set(incoming.denom, incoming);
            continue;
        }

        const firstSeen = mergeFirstSeen(
            existing.first_seen_height,
            existing.first_seen_tx,
            incoming.first_seen_height,
            incoming.first_seen_tx,
        );

        mergedByDenom.set(incoming.denom, {
            denom: incoming.denom,
            type: chooseType(existing.type, incoming.type),
            base_denom: preferIncomingNonEmpty(existing.base_denom, incoming.base_denom),
            symbol: preferIncomingNonEmpty(existing.symbol, incoming.symbol),
            decimals: incoming.decimals ?? existing.decimals,
            creator: preferIncomingNonEmpty(existing.creator, incoming.creator),
            first_seen_height: firstSeen.height,
            first_seen_tx: firstSeen.tx,
            metadata: mergeMetadata(existing.metadata, incoming.metadata),
        });
    }

    const uniqueRows = Array.from(mergedByDenom.values());
    if (uniqueRows.length === 0) return;

    await execBatchedInsert(
        client,
        'tokens.registry',
        cols,
        uniqueRows,
        'ON CONFLICT (denom) DO UPDATE SET ' +
        // Allow correcting older misclassified rows (e.g., native -> ibc/cw20/factory).
        'type = CASE ' +
        'WHEN tokens.registry.type = \'native\' AND EXCLUDED.type <> \'native\' THEN EXCLUDED.type ' +
        'ELSE tokens.registry.type END, ' +
        'base_denom = COALESCE(EXCLUDED.base_denom, tokens.registry.base_denom), ' +
        'symbol = COALESCE(EXCLUDED.symbol, tokens.registry.symbol), ' +
        'decimals = COALESCE(EXCLUDED.decimals, tokens.registry.decimals), ' +
        'creator = COALESCE(EXCLUDED.creator, tokens.registry.creator), ' +
        'first_seen_height = CASE ' +
        'WHEN tokens.registry.first_seen_height IS NULL THEN EXCLUDED.first_seen_height ' +
        'WHEN EXCLUDED.first_seen_height IS NULL THEN tokens.registry.first_seen_height ' +
        'ELSE LEAST(tokens.registry.first_seen_height, EXCLUDED.first_seen_height) END, ' +
        'first_seen_tx = CASE ' +
        'WHEN tokens.registry.first_seen_height IS NULL AND EXCLUDED.first_seen_height IS NOT NULL THEN COALESCE(EXCLUDED.first_seen_tx, tokens.registry.first_seen_tx) ' +
        'WHEN EXCLUDED.first_seen_height IS NULL THEN COALESCE(tokens.registry.first_seen_tx, EXCLUDED.first_seen_tx) ' +
        'WHEN tokens.registry.first_seen_height IS NULL THEN COALESCE(EXCLUDED.first_seen_tx, tokens.registry.first_seen_tx) ' +
        'WHEN EXCLUDED.first_seen_height < tokens.registry.first_seen_height THEN COALESCE(EXCLUDED.first_seen_tx, tokens.registry.first_seen_tx) ' +
        'WHEN EXCLUDED.first_seen_height = tokens.registry.first_seen_height THEN COALESCE(tokens.registry.first_seen_tx, EXCLUDED.first_seen_tx) ' +
        'ELSE tokens.registry.first_seen_tx END, ' +
        'metadata = NULLIF(COALESCE(tokens.registry.metadata, \'{}\'::jsonb) || COALESCE(EXCLUDED.metadata, \'{}\'::jsonb), \'{}\'::jsonb), ' +
        'updated_at = NOW()'
    );
}
