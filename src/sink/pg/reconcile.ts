import { PoolClient } from 'pg';
import { getLogger } from '../../utils/logger.js';
import { insertBalanceDeltas } from './inserters/bank.js';
import { RpcClient } from '../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../decode/dynamicProto.js';
import Long from 'long';

const log = getLogger('sink/reconcile');

/**
 * Reconciles negative balances by fetching the true state from RPC and inserting a correction delta.
 * @param client - Database client
 * @param rpc - RPC Client for querying balances via ABCI
 * @param protoRoot - Protobuf Root for encoding/decoding
 */
export async function reconcileNegativeBalances(client: PoolClient, rpc: RpcClient, protoRoot: Root) {
    log.info('[reconcile] Starting reconciliation of negative balances...');

    // 1. Identify Negative Accounts
    const res = await client.query(`
    SELECT account, key as denom, value as current_balance_str
    FROM bank.balances_current, jsonb_each_text(balances)
    WHERE (value::NUMERIC < 0);
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
            const trueBalance = await fetchBalanceViaAbci(rpc, protoRoot, account, denom);
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

// Helper to fetch specific denom balance using ABCI Query (works on Tendermint RPC port)
async function fetchBalanceViaAbci(rpc: RpcClient, root: Root, address: string, denom: string): Promise<bigint> {
    const ReqType = root.lookupType('cosmos.bank.v1beta1.QueryBalanceRequest');
    const ResType = 'cosmos.bank.v1beta1.QueryBalanceResponse'; // String for decodeAnyWithRoot

    // Encode request
    const reqMsg = ReqType.create({ address, denom });
    const reqBytes = ReqType.encode(reqMsg).finish();
    // ✅ Fix: Tendermint RPC requires '0x' prefix for hex-encoded data arguments
    const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

    // Query ABCI
    const path = '/cosmos.bank.v1beta1.Query/Balance';

    // ✅ Final Fix: Send data WITHOUT extra quotes - certain Tendermint versions are picky
    let response: any;
    try {
        const params = {
            path: `"${path}"`,
            data: reqHex
        };
        const j = await rpc.getJson('/abci_query', params);
        response = j.result?.response ?? j;
    } catch (err: any) {
        // Fallback to quoted (standard) if raw fails (defensive)
        response = await rpc.queryAbci(path, reqHex);
    }

    // ✅ FIX: Simplified response validation logic
    if (!response) {
        log.warn(`[reconcile] Empty ABCI response for ${address}/${denom}`);
        return 0n;
    }
    if (response.code !== 0) {
        throw new Error(`ABCI Error ${response.code}: ${response.log}`);
    }
    if (!response.value) {
        // Account exists but has 0 balance for this denom
        return 0n;
    }

    // Decode response
    const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), root);
    const balance = (decoded as any).balance;

    return BigInt(balance?.amount || '0');
}
