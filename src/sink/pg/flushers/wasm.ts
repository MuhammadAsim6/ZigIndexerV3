import type { PoolClient } from 'pg';
import { insertWasmCodes, insertWasmContracts, insertWasmMigrations } from '../inserters/wasm.js';

export async function flushWasmRegistry(
    client: PoolClient,
    data: { codes: any[]; contracts: any[]; migrations: any[] }
): Promise<void> {
    // Safety timeout
    await client.query(`SET LOCAL statement_timeout = '30s'`);

    if (data.codes.length > 0) {
        await insertWasmCodes(client, data.codes);
    }
    if (data.contracts.length > 0) {
        await insertWasmContracts(client, data.contracts);
    }
    if (data.migrations.length > 0) {
        await insertWasmMigrations(client, data.migrations);
    }
}
