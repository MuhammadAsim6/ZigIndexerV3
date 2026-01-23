import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

/**
 * Insert WASM swap events from CosmWasm AMM contracts (e.g., Astroport-style).
 * Extracts detailed swap data from WASM event attributes.
 */
export async function insertWasmSwaps(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'tx_hash',
        'msg_index',
        'event_index',
        'contract',
        'sender',
        'receiver',
        'offer_asset',
        'ask_asset',
        'offer_amount',
        'return_amount',
        'spread_amount',
        'commission_amount',
        'maker_fee_amount',
        'fee_share_amount',
        'reserves',
        'pair_id',
        'effective_price',
        'price_impact',
        'total_fee',
        'block_height',
        'timestamp'
    ];



    const { text, values } = makeMultiInsert(
        'wasm.dex_swaps',
        cols,
        rows,
        'ON CONFLICT (tx_hash, msg_index, event_index, block_height) DO NOTHING'
    );

    await client.query(text, values);
}

/**
 * Insert factory tokens discovered from denom patterns.
 */
export async function insertFactoryTokens(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;

    const cols = [
        'denom',
        'base_denom',
        'creator',
        'symbol',
        'first_seen_height',
        'first_seen_tx'
    ];

    const { text, values } = makeMultiInsert(
        'tokens.factory_tokens',
        cols,
        rows,
        'ON CONFLICT (denom) DO NOTHING'
    );

    await client.query(text, values);
}
