import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

export async function insertWasmCodes(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['code_id', 'checksum', 'creator', 'instantiate_permission', 'store_tx_hash', 'store_height'];
    const { text, values } = makeMultiInsert(
        'wasm.codes',
        cols,
        rows,
        'ON CONFLICT (code_id) DO NOTHING',
        { instantiate_permission: 'jsonb' }
    );
    await client.query(text, values);
}

export async function insertWasmContracts(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['address', 'code_id', 'creator', 'admin', 'label', 'created_height', 'created_tx_hash'];
    const { text, values } = makeMultiInsert(
        'wasm.contracts',
        cols,
        rows,
        'ON CONFLICT (address) DO NOTHING'
    );
    await client.query(text, values);
}

export async function insertWasmMigrations(client: PoolClient, rows: any[]): Promise<void> {
    if (!rows?.length) return;
    const cols = ['contract', 'from_code_id', 'to_code_id', 'height', 'tx_hash'];
    const { text, values } = makeMultiInsert(
        'wasm.contract_migrations',
        cols,
        rows,
        'ON CONFLICT (contract, height, tx_hash) DO NOTHING'
    );
    await client.query(text, values);
}
