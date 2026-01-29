// src/sink/pg/reconcile.ts
import { PoolClient } from 'pg';
import { getLogger } from '../../utils/logger.js';
const log = getLogger('sink/reconcile');
import { insertBalanceDeltas } from './inserters/bank.js';

/**
 * Reconciles negative balances by fetching the true state from RPC and inserting a correction delta.
 * @param client - Database client
 * @param rpcUrl - RPC URL for querying balances
 */
export async function reconcileNegativeBalances(client: PoolClient, rpcUrl: string) {
    log.info('[reconcile] Starting reconciliation of negative balances...');

    // 1. Identify Negative Accounts
    const res = await client.query(`
    SELECT account, key as denom, value as current_balance_str
    FROM bank.balances_current, jsonb_each_text(balances)
    WHERE value LIKE '-%'
    LIMIT 50;
  `);

    if (res.rowCount === 0) {
        log.info('[reconcile] No negative balances found. System is healthy.');
        return;
    }

    log.info(`[reconcile] Found ${res.rowCount} negative entries. Processing...`);

    const correctionDeltas: any[] = [];

    // Query max height for insertion
    const hRes = await client.query('SELECT max(height) as h FROM core.blocks');
    const currentHeight = Number(hRes.rows[0]?.h || 0);

    for (const row of res.rows) {
        const { account, denom, current_balance_str } = row;

        try {
            const trueBalance = await fetchAccountBalanceIds(rpcUrl, account, denom);
            const currentDbBalance = BigInt(current_balance_str);
            const diff = trueBalance - currentDbBalance;

            if (diff !== 0n) {
                log.info(`[reconcile] Correcting ${account} (${denom}): DB=${currentDbBalance}, RPC=${trueBalance}, Delta=${diff}`);

                correctionDeltas.push({
                    height: currentHeight,
                    account,
                    denom,
                    delta: diff.toString(),
                    tx_hash: 'reconcile_auto_heal', // ✅ Explicit source
                    msg_index: -1,
                    event_index: 0
                });
            }

        } catch (err: any) {
            log.error(`[reconcile] Failed to reconcile ${account}: ${err.message}`);
        }
    }

    // 4. flush corrections
    if (correctionDeltas.length > 0) {
        await insertBalanceDeltas(client, correctionDeltas);
        log.info(`[reconcile] Successfully applied ${correctionDeltas.length} corrections.`);
    }
}

// Helper to fetch specific denom balance
async function fetchAccountBalanceIds(rpcUrl: string, address: string, denom: string): Promise<bigint> {
    const url = `${rpcUrl}/cosmos/bank/v1beta1/balances/${address}/by_denom?denom=${denom}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`RPC ${resp.status}`);
    const data = await resp.json() as any;
    return BigInt(data?.balance?.amount || '0');
}
