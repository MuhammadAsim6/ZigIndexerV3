import fs from 'node:fs';
import path from 'node:path';
import { getPgPool, closePgPool } from '../db/pg.js';
import { getLogger } from '../utils/logger.js';
import { getConfig } from '../config.js';

const log = getLogger('scripts/genesis-bootstrap');

/**
 * Enhanced Genesis Bootstrap Script
 * Captures:
 * - Bank balances (standard)
 * - Staking delegations (bonded tokens - informational)
 * - Vesting accounts (locked + vested - informational)
 * - Supply verification
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

    const bankBalances = genesis.app_state?.bank?.balances ?? [];
    const stakingDelegations = genesis.app_state?.staking?.delegations ?? [];
    const vestingAccounts = genesis.app_state?.auth?.accounts?.filter(
        (acc: any) => acc['@type']?.includes('Vesting') || acc.base_vesting_account
    ) ?? [];
    const totalSupply = genesis.app_state?.bank?.supply ?? [];

    if (!Array.isArray(bankBalances) || bankBalances.length === 0) {
        log.error('Invalid genesis format: app_state.bank.balances is missing or empty');
        return;
    }

    log.info(`Found: ${bankBalances.length} bank accounts, ${stakingDelegations.length} delegations, ${vestingAccounts.length} vesting accounts`);

    const config = getConfig();
    const pool = getPgPool({ ...config.pg, applicationName: 'genesis-bootstrap' });
    const client = await pool.connect();

    try {
        // Check if we already have genesis data
        const checkRes = await client.query('SELECT 1 FROM bank.balance_deltas WHERE height = 0 LIMIT 1');
        if (checkRes.rowCount && checkRes.rowCount > 0) {
            log.info('Genesis balances already exist (Height 0 found). Skipping.');
            return;
        }

        await client.query('BEGIN');

        // 1. Bank Balances (liquid balances)
        const rows: any[] = [];
        const balanceTracker = new Map<string, bigint>(); // Track total per denom for verification

        for (const bal of bankBalances) {
            const address = bal.address;
            for (const coin of bal.coins ?? []) {
                rows.push({
                    height: 0,
                    account: address,
                    denom: coin.denom,
                    delta: coin.amount,
                    tx_hash: 'genesis_bank',
                    msg_index: 0,
                    event_index: 0
                });
                // Track for verification
                const current = balanceTracker.get(coin.denom) ?? 0n;
                balanceTracker.set(coin.denom, current + BigInt(coin.amount));
            }
        }
        log.info(`Prepared ${rows.length} bank balance entries.`);

        // 2. Staking Delegations (bonded tokens - informational only)
        // Note: Delegated tokens are NOT additional liquid balances
        // They represent tokens bonded to validators from the delegator's account
        let totalDelegated = 0n;
        for (const del of stakingDelegations) {
            if (del.shares) {
                // shares are typically equal to tokens at genesis
                totalDelegated += BigInt(Math.floor(parseFloat(del.shares)));
            }
        }
        log.info(`Total delegated tokens at genesis: ${totalDelegated.toString()} (informational - not added to balances)`);

        // 3. Vesting Accounts (track original vesting for reference)
        // Vesting tokens ARE included in bank balances, this is informational
        let totalVesting = 0n;
        for (const acc of vestingAccounts) {
            const vestingAcc = acc.base_vesting_account ?? acc;
            const originalVesting = vestingAcc.original_vesting ?? [];
            for (const coin of originalVesting) {
                totalVesting += BigInt(coin.amount);
            }
        }
        log.info(`Total vesting tokens at genesis: ${totalVesting.toString()} (included in bank balances)`);

        // 4. Supply Verification
        if (totalSupply.length > 0) {
            log.info('Verifying total supply...');
            for (const supply of totalSupply) {
                const tracked = balanceTracker.get(supply.denom) ?? 0n;
                const expected = BigInt(supply.amount);
                if (tracked !== expected) {
                    log.warn(`[supply mismatch] ${supply.denom}: tracked=${tracked}, expected=${expected}, diff=${expected - tracked}`);
                } else {
                    log.info(`[supply verified] ${supply.denom}: ${tracked.toString()}`);
                }
            }
        }

        if (rows.length === 0) {
            log.warn('No balances found to insert.');
            await client.query('ROLLBACK');
            return;
        }

        // 5. Batch Insert into bank.balance_deltas (Chunked)
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

        // 6. Trigger Refresh
        await client.query('SELECT bank.populate_balances_current()');

        await client.query('COMMIT');
        log.info('✅ Genesis bootstrap complete!');

    } catch (err: any) {
        await client.query('ROLLBACK');
        log.error(`Bootstrap failed: ${err.message}`);
    } finally {
        client.release();
    }
}

// Support standalone execution if called directly
import { fileURLToPath } from 'node:url';
if (process.argv[1] === fileURLToPath(import.meta.url)) {
    const args = process.argv.slice(2);
    const genesisPath = args[0] || process.env.GENESIS_PATH;
    if (genesisPath) {
        bootstrapGenesis(genesisPath).then(() => {
            closePgPool();
        });
    } else {
        console.log('Usage: npx tsx src/scripts/genesis-bootstrap.ts <path-to-genesis.json>');
    }
}
