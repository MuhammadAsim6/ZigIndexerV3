import fs from 'node:fs';
import path from 'node:path';
import { getPgPool, closePgPool } from '../db/pg.js';
import { getLogger } from '../utils/logger.js';
import { getConfig } from '../config.js';

const log = getLogger('scripts/genesis-bootstrap');

/**
 * Script to bootstrap bank balances from a genesis file.
 * Usage: npx tsx src/scripts/genesis-bootstrap.ts <path-to-genesis.json>
 */
/**
 * Script to bootstrap bank balances from a genesis file.
 * Can be run standalone or imported.
 */
export async function bootstrapGenesis(genesisPath: string) {
    if (!genesisPath) {
        log.error('Usage: bootstrapGenesis(<path-to-genesis.json>)');
        return;
    }

    log.info(`Reading genesis file from: ${genesisPath}`);

    let genesis: any;
    try {
        if (!fs.existsSync(genesisPath)) {
            log.warn(`Genesis file not found at ${genesisPath}. Skipping bootstrap.`);
            return;
        }
        const raw = fs.readFileSync(path.resolve(genesisPath), 'utf8');
        genesis = JSON.parse(raw);
    } catch (err: any) {
        log.error(`Failed to read genesis file: ${err.message}`);
        return;
    }

    const balances = genesis.app_state?.bank?.balances;
    if (!Array.isArray(balances)) {
        log.error('Invalid genesis format: app_state.bank.balances is missing or not an array');
        return;
    }

    log.info(`Found ${balances.length} accounts with initial balances.`);

    const config = getConfig();
    const pool = getPgPool({ ...config.pg, applicationName: 'genesis-bootstrap' });
    const client = await pool.connect();

    try {
        // Check if we already have genesis data (heuristic: check if height 0 exists)
        const checkRes = await client.query('SELECT 1 FROM bank.balance_deltas WHERE height = 0 LIMIT 1');
        if (checkRes.rowCount && checkRes.rowCount > 0) {
            log.info('Genesis balances already exist (Height 0 found). Skipping.');
            return;
        }

        await client.query('BEGIN');

        // 1. Prepare Delta Rows
        const rows: any[] = [];
        for (const bal of balances) {
            const address = bal.address;
            for (const coin of bal.coins) {
                rows.push({
                    height: 0,
                    account: address,
                    denom: coin.denom,
                    delta: coin.amount // Positive delta for initial balance
                });
            }
        }

        if (rows.length === 0) {
            log.warn('No balances found to insert.');
            await client.query('ROLLBACK');
            return;
        }

        log.info(`Prepared ${rows.length} balance entries. Inserting into bank.balance_deltas...`);

        // 2. Batch Insert into bank.balance_deltas (Chunked)
        const BATCH_SIZE = 5000;
        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const values: any[] = [];
            const placeholders: string[] = [];
            let idx = 1;

            for (const row of batch) {
                placeholders.push(`($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}::numeric)`);
                values.push(row.height, row.account, row.denom, row.delta);
                idx += 4;
            }

            await client.query(`
        INSERT INTO bank.balance_deltas (height, account, denom, delta)
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (height, account, denom) DO UPDATE SET delta = bank.balance_deltas.delta + EXCLUDED.delta
      `, values);

            log.info(`Inserted batch ${i / BATCH_SIZE + 1}/${Math.ceil(rows.length / BATCH_SIZE)}`);
        }

        log.info('All deltas inserted. Refreshing current balances view...');

        // 3. Trigger Refresh
        await client.query('SELECT bank.populate_balances_current()');

        await client.query('COMMIT');
        log.info('✅ Genesis bootstrap complete!');

    } catch (err: any) {
        await client.query('ROLLBACK');
        log.error(`Bootstrap failed: ${err.message}`);
    } finally {
        client.release();
        // Do not close pool here if imported, let main app handle it? 
        // Actually, getting a new pool from getPgPool might reuse the singleton if configured.
        // However, looking at db/pg.ts, getPgPool creates a singleton.
        // We probably shouldn't close it if we are part of the main app flow, 
        // OR we should rely on the main app's pool. 
        // For safety in this hybrid script, let's NOT close the global pool if imported.
        // But since we used getPgPool with a custom applicationName, it might have created a new one or replaced the global?
        // Let's assume for now we shouldn't close it to avoid killing the main app's connection.
        // await closePgPool(); 
    }
}

// Support standalone execution if called directly
import { fileURLToPath } from 'node:url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    const args = process.argv.slice(2);
    const genesisPath = args[0] || process.env.GENESIS_PATH;
    if (genesisPath) {
        bootstrapGenesis(genesisPath).then(() => {
            // For standalone, we DO want to close the pool
            closePgPool();
        });
    } else {
        // console.log('Usage: npx tsx src/scripts/genesis-bootstrap.ts <path-to-genesis.json>');
    }
}
