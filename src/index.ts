/**
 * Entry point for the Cosmos indexer application.
 */
// src/index.ts
import { EventEmitter } from 'node:events';
import { getConfig, printConfig } from './config.ts';
import { createRpcClientFromConfig } from './rpc/client.ts';
import { createTxDecodePool } from './decode/txPool.ts';
import { createSink } from './sink/index.ts';
import { closePgPool, createPgPool, getPgPool } from './db/pg.ts';
import { getProgress } from './db/progress.ts';
import { upsertValidators } from './sink/pg/flushers/validators.ts';
import { insertWrapperSettings } from './sink/pg/inserters/zigchain.ts';
import { insertTokenRegistry } from './sink/pg/inserters/tokens.ts';
import { deriveConsensusAddress } from './utils/crypto.ts';
import { loadProtoRoot, decodeAnyWithRoot } from './decode/dynamicProto.ts';
import { base64ToBytes } from './utils/bytes.ts';
import { buildTokenRegistryRow } from './utils/token-registry.js';
import path from 'node:path';
import { getLogger } from './utils/logger.ts';
import { syncRange } from './runner/syncRange.ts';
import { retryMissingBlocks } from './runner/retryMissing.ts';
import { followLoop } from './runner/follow.ts';
import { bootstrapGenesis } from './scripts/genesis-bootstrap.ts';
import { runReconcileCycle } from './sink/pg/reconcile.ts';
import { ensureCorePartitions } from './db/partitions.js';

EventEmitter.defaultMaxListeners = 0;
const log = getLogger('index');

function normalizeNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeNonNegativeInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n < 0 || n > 2147483647) return null;
  return n;
}

async function fetchAllStakingValidatorsViaAbci(rpc: any, protoRoot: any): Promise<any[]> {
  const requestType = protoRoot.lookupType('cosmos.staking.v1beta1.QueryValidatorsRequest');
  const responseType = 'cosmos.staking.v1beta1.QueryValidatorsResponse';
  const path = '/cosmos.staking.v1beta1.Query/Validators';
  const out: any[] = [];
  const seen = new Set<string>();
  let nextKey: string | null = null;
  const maxPages = 200;

  for (let page = 0; page < maxPages; page++) {
    const payload: any = {
      status: 'BOND_STATUS_UNSPECIFIED',
      pagination: { limit: 500 },
    };
    if (nextKey) payload.pagination.key = base64ToBytes(nextKey);

    const req = requestType.create(payload);
    const reqHex = '0x' + Buffer.from(requestType.encode(req).finish()).toString('hex');
    const abciRes = await rpc.queryAbci(path, reqHex);
    if (!abciRes?.value) break;

    const decoded = decodeAnyWithRoot(responseType, base64ToBytes(abciRes.value), protoRoot) as any;
    const validators = Array.isArray(decoded?.validators) ? decoded.validators : [];
    for (const v of validators) {
      const op = String(v?.operator_address || v?.operatorAddress || v?.address || v?.operator_addr || '');
      if (!op || seen.has(op)) continue;
      seen.add(op);
      out.push(v);
    }

    const nk = decoded?.pagination?.next_key ?? decoded?.pagination?.nextKey ?? null;
    if (typeof nk === 'string' && nk.length > 0) {
      nextKey = nk;
    } else {
      break;
    }
  }

  return out;
}

async function main() {
  const cfg = getConfig();
  log.info(`[start] Cosmos Indexer v1.1.0-reconcile-refactor starting...`);
  printConfig(cfg);

  let decodePool: any = null;
  let sink: any = null;
  let reconcileTimer: NodeJS.Timeout | null = null;

  // Cleanup handler for signals and catch blocks
  let isCleaningUp = false;
  const gracefulCleanup = async (exitCode: number) => {
    if (isCleaningUp) return;
    isCleaningUp = true;
    await cleanup(decodePool, sink, reconcileTimer);
    process.exit(exitCode);
  };

  process.on('SIGINT', () => {
    log.info('SIGINT received. Cleaning up...');
    gracefulCleanup(0);
  });
  process.on('SIGTERM', () => {
    log.info('SIGTERM received. Cleaning up...');
    gracefulCleanup(0);
  });

  try {
    // 🛡️ INITIALIZE DB POOL EARLY
    if (cfg.sinkKind === 'postgres') {
      createPgPool({ ...cfg.pg, applicationName: 'cosmos-indexer' });
    }

    // 🛡️ GENESIS BOOTSTRAP (Auto-run if genesis.json exists)
    const genesisPath = process.env.GENESIS_PATH || '/usr/src/app/genesis.json';
    await bootstrapGenesis(genesisPath);

    const rpc = createRpcClientFromConfig(cfg);
    const status = await rpc.fetchStatus();

    let startFrom = cfg.from as number | undefined;
    let lastProgress: number | null = null;
    const wantResume =
      !startFrom || cfg.resume === true || (typeof cfg.from === 'string' && cfg.from.toLowerCase() === 'resume');

    if (wantResume) {
      if (cfg.sinkKind === 'postgres') {
        const pool = getPgPool();
        try {
          const last = await getProgress(pool, cfg.pg?.progressId ?? 'default');
          lastProgress = last;
          const earliest = Number(status['sync_info']['earliest_block_height']);
          const explicitFrom = typeof cfg.from === 'number' ? cfg.from : (cfg.firstBlock as number | undefined);
          startFrom = last != null ? last + 1 : (explicitFrom ?? earliest);
          log.info(`[resume] last_height=${last ?? 'null'} → start from ${startFrom}`);
        } catch (err) {
          log.warn(`[resume] check failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      } else {
        log.warn('[resume] requested, but sinkKind is not postgres — skipping.');
      }
    }

    let endHeight = cfg.to as number | undefined;
    if (!endHeight) {
      endHeight = Number(status['sync_info']['latest_block_height']);
      log.info(`[config] --to not provided → using latest height ${endHeight}`);
    }
    if (startFrom == null || endHeight == null) throw new Error('Both startFrom and endHeight must be resolved.');
    log.info(`[start] from ${startFrom} to ${endHeight} (incl.)`);
    if (cfg.sinkKind === 'postgres' && lastProgress == null && startFrom > 1) {
      log.warn(
        `[start] starting above height 1 (from=${startFrom}) with no existing progress. Historical bank/factory state before this height will be missing; prefer FIRST_BLOCK=1 for fully accurate balances.`,
      );
    }

    const protoDir = process.env.PROTO_DIR || path.join(process.cwd(), 'protos');
    log.info(`[proto] dir = ${protoDir}`);

    const poolSize = Math.max(1, Math.min(cfg.concurrency ?? 12, 12));
    decodePool = createTxDecodePool(poolSize, { protoDir });

    sink = createSink({
      kind: cfg.sinkKind,
      outPath: cfg.outPath,
      flushEvery: cfg.flushEvery ?? 1,
      pg: cfg.pg,
      batchSizes: {
        blocks: cfg.pg?.batchBlocks,
        txs: cfg.pg?.batchTxs,
        msgs: cfg.pg?.batchMsgs,
        events: cfg.pg?.batchEvents,
        attrs: cfg.pg?.batchAttrs,
      },
      rpcUrl: cfg.rpcUrl,
    });
    await sink.init();

    // 🛡️ INITIAL SYNC: Fetch validators on startup
    if (cfg.sinkKind === 'postgres') {
      try {
        log.info('[start] syncing staking validators…');
        const protoRoot = await loadProtoRoot(protoDir);
        const pool = getPgPool();
        const client = await pool.connect();
        try {
          const stakingVals = await fetchAllStakingValidatorsViaAbci(rpc, protoRoot);
          if (stakingVals.length > 0) {
            log.info(`[start] found ${stakingVals.length} staking validators`);
          }

          const rows = [];
          for (const v of (stakingVals as any[])) {
              const opAddr = v.operator_address || v.operatorAddress || v.address || v.operator_addr;
              if (!opAddr) continue;

              let consAddr: string | null = null;
              const pubAny = v.consensus_pubkey || v.consensusPubkey || v.consensusPubKey || v.consensus_pub_key;
              if (pubAny?.value) {
                try {
                  let rawVal: Uint8Array;
                  if (typeof pubAny.value === 'string') {
                    rawVal = base64ToBytes(pubAny.value);
                  } else if (typeof pubAny.value === 'object' && pubAny.value !== null) {
                    rawVal = new Uint8Array(Object.values(pubAny.value));
                  } else {
                    throw new Error('Unsupported pubAny.value type: ' + typeof pubAny.value);
                  }

                  const decodedPub = decodeAnyWithRoot(pubAny['@type'] || pubAny.type_url || pubAny.typeUrl, rawVal, protoRoot);
                  const rawKey = decodedPub.key || decodedPub.value;

                  if (rawKey) {
                    const rawKeyB64 = (typeof rawKey === 'string')
                      ? rawKey
                      : Buffer.from(Object.values(rawKey) as any).toString('base64');

                    consAddr = deriveConsensusAddress(rawKeyB64);
                    log.info(`[start] derived consAddr ${consAddr} for ${opAddr}`);
                  }
                } catch (e) {
                  log.warn(`Failed to derive consAddr for ${opAddr}: ${e}`);
                }
              }

              const commRates = v.commission?.commissionRates || v.commission?.commission_rates;

              const toDecimal = (val: string | number | null | undefined): string | null => {
                if (val == null) return null;
                try {
                  const str = String(val);
                  if (str.includes('.') && parseFloat(str) < 100) return str;
                  const num = BigInt(str);
                  const divisor = BigInt('1000000000000000000');
                  const intPart = num / divisor;
                  const fracPart = num % divisor;
                  const fracStr = fracPart.toString().padStart(18, '0');
                  return `${intPart}.${fracStr}`;
                } catch { return null; }
              };

              let rawPubkeyB64: string | null = null;
              if (pubAny?.value) {
                try {
                  if (typeof pubAny.value === 'string') {
                    rawPubkeyB64 = pubAny.value;
                  } else if (typeof pubAny.value === 'object' && pubAny.value !== null) {
                    rawPubkeyB64 = Buffer.from(Object.values(pubAny.value) as any).toString('base64');
                  }
                } catch (e) { /* ignore */ }
              }

            rows.push({
              operator_address: opAddr,
              consensus_address: consAddr,
              consensus_pubkey: rawPubkeyB64,
              moniker: v.description?.moniker || `Validator ${String(opAddr).slice(-8)}`,
              website: v.description?.website,
              details: v.description?.details,
              commission_rate: toDecimal(commRates?.rate || commRates?.Rate),
              max_commission_rate: toDecimal(commRates?.maxRate || commRates?.max_rate || commRates?.MaxRate),
              max_change_rate: toDecimal(commRates?.maxChangeRate || commRates?.max_change_rate || commRates?.MaxChangeRate),
              min_self_delegation: v.min_self_delegation || v.minSelfDelegation,
              status: v.status,
              updated_at_height: startFrom,
              updated_at_time: new Date()
            });
          }

          if (rows.length > 0) {
            await client.query('BEGIN');
            await client.query('DELETE FROM core.validators');
            await upsertValidators(client, rows);
            await client.query('COMMIT');
            log.info(`[start] synced ${rows.length} validators`);
          }
        } finally {
          client.release();
        }
      } catch (err) {
        log.warn('[start] validator sync failed:', err instanceof Error ? err.message : String(err));
      }
    }

    // 🛡️ INITIAL SYNC: Fetch network parameters
    try {
      const pool = getPgPool();
      const client = await pool.connect();
      try {
        await ensureCorePartitions(client, startFrom, startFrom);

        const modules = [
          { name: 'auth', path: '/cosmos.auth.v1beta1.Query/Params', resp: 'cosmos.auth.v1beta1.QueryParamsResponse' },
          { name: 'bank', path: '/cosmos.bank.v1beta1.Query/Params', resp: 'cosmos.bank.v1beta1.QueryParamsResponse' },
          { name: 'staking', path: '/cosmos.staking.v1beta1.Query/Params', resp: 'cosmos.staking.v1beta1.QueryParamsResponse' },
          { name: 'distribution', path: '/cosmos.distribution.v1beta1.Query/Params', resp: 'cosmos.distribution.v1beta1.QueryParamsResponse' },
          { name: 'mint', path: '/cosmos.mint.v1beta1.Query/Params', resp: 'cosmos.mint.v1beta1.QueryParamsResponse' },
          { name: 'slashing', path: '/cosmos.slashing.v1beta1.Query/Params', resp: 'cosmos.slashing.v1beta1.QueryParamsResponse' },
          { name: 'factory', path: '/zigchain.factory.Query/Params', resp: 'zigchain.factory.QueryParamsResponse' },
          { name: 'gov', path: '/cosmos.gov.v1.Query/Params', resp: 'cosmos.gov.v1.QueryParamsResponse' },
        ];

        const rows = [];
        for (const mod of modules) {
          try {
            const abci = await rpc.queryAbci(mod.path);
            if (abci?.value) {
              const decoded = await decodePool.decodeGeneric(mod.resp, abci.value);
              const params = decoded.params || decoded.voting_params || decoded.tally_params || decoded.deposit_params || decoded;
              if (params) {
                // ✅ ENHANCEMENT: Update governance parameters helper for accurate fallbacks
                if (mod.name === 'gov') {
                  try {
                    const { updateGovParams, parseDurationToMs } = await import('./utils/gov_params.js');
                    updateGovParams({
                      maxDepositPeriodMs: parseDurationToMs(params.max_deposit_period) ?? undefined,
                      votingPeriodMs: parseDurationToMs(params.voting_period) ?? undefined,
                      expeditedVotingPeriodMs: parseDurationToMs(params.expedited_voting_period) ?? undefined,
                    });
                    log.info(`[start] synchronized gov params: maxDeposit=${params.max_deposit_period}, voting=${params.voting_period}`);
                  } catch (e) {
                    log.warn(`[start] failed to update gov params helper: ${e}`);
                  }
                }

                rows.push({
                  height: startFrom,
                  time: new Date(),
                  module: mod.name,
                  param_key: '_all',
                  old_value: null,
                  new_value: params
                });
              }
            }
          } catch (e) {
            log.debug(`[start] skip module ${mod.name}`);
          }
        }

        if (rows.length > 0) {
          const { flushNetworkParams } = await import('./sink/pg/flushers/params.js');
          await client.query('BEGIN');
          await flushNetworkParams(client, rows);
          await client.query('COMMIT');
          log.info(`[start] synced parameters for ${rows.length} modules`);
        }
      } finally {
        client.release();
      }
    } catch (err) {
      log.warn('[start] params sync failed');
    }

    // 🛡️ INITIAL SYNC: Fetch tokenwrapper module info snapshot
    try {
      const pool = getPgPool();
      const client = await pool.connect();
      try {
        const abci = await rpc.queryAbci('/zigchain.tokenwrapper.Query/ModuleInfo');
        if (abci?.value) {
          const decoded = await decodePool.decodeGeneric('zigchain.tokenwrapper.QueryModuleInfoResponse', abci.value);
          const row = {
            denom: normalizeNonEmptyString(decoded?.denom),
            native_client_id: normalizeNonEmptyString(decoded?.native_client_id),
            counterparty_client_id: normalizeNonEmptyString(decoded?.counterparty_client_id),
            native_port: normalizeNonEmptyString(decoded?.native_port),
            counterparty_port: normalizeNonEmptyString(decoded?.counterparty_port),
            native_channel: normalizeNonEmptyString(decoded?.native_channel),
            counterparty_channel: normalizeNonEmptyString(decoded?.counterparty_channel),
            decimal_difference: normalizeNonNegativeInt(decoded?.decimal_difference),
            updated_at_height: startFrom,
          };

          if (!row.denom) {
            log.warn('[start] tokenwrapper module info returned empty denom; skipping wrapper_settings bootstrap');
          } else {
            await client.query('BEGIN');
            await insertWrapperSettings(client, [row]);
            const registryRow = buildTokenRegistryRow({
              denom: row.denom,
              height: Number(startFrom ?? 0),
              txHash: null,
              source: 'wrapper_settings',
            });
            if (registryRow) {
              await insertTokenRegistry(client, [registryRow]);
            }
            await client.query('COMMIT');
            log.info(`[start] synced tokenwrapper settings for denom=${row.denom}`);
          }
        }
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw e;
      } finally {
        client.release();
      }
    } catch (err) {
      log.warn('[start] tokenwrapper module info sync failed');
    }


    const backfill = await syncRange(rpc, decodePool, sink, {
      from: startFrom,
      to: endHeight,
      concurrency: cfg.concurrency,
      progressEveryBlocks: cfg.progressEveryBlocks,
      progressIntervalSec: cfg.progressIntervalSec,
      caseMode: cfg.caseMode,
    });

    log.info(`[done-range] processed ${backfill.processed} blocks`);

    if (cfg.sinkKind === 'postgres') {
      await retryMissingBlocks(rpc, decodePool, sink, {
        concurrency: Math.max(1, Math.min(cfg.concurrency ?? 8, 8)),
        caseMode: cfg.caseMode,
      });
    }

    if (cfg.sinkKind === 'postgres' && cfg.reconcileMode !== 'off') {
      try {
        const pool = getPgPool();
        const client = await pool.connect();
        try {
          const protoRoot = await loadProtoRoot(protoDir);
          await runReconcileCycle(client, rpc, protoRoot, {
            mode: cfg.reconcileMode ?? 'full-once-then-negative',
            maxLagBlocks: cfg.reconcileMaxLagBlocks ?? 10_000,
            fullBatchSize: cfg.reconcileFullBatchSize ?? 200,
            stateId: cfg.reconcileStateId ?? 'default',
          });
        } finally {
          client.release();
        }
      } catch (err) {
        log.error('[reconcile] initial cycle error:', err instanceof Error ? err.message : String(err));
      }
    }

    if (cfg.follow !== false) {
      const pollMs = cfg.followIntervalMs ?? 1500;

      if (cfg.sinkKind === 'postgres' && cfg.reconcileMode !== 'off') {
        const periodicMode = cfg.reconcileMode === 'full-once-then-negative' ? 'negative-only' : cfg.reconcileMode;
        let reconcileRunning = false;
        reconcileTimer = setInterval(async () => {
          if (reconcileRunning) {
            log.warn('[reconcile] previous cycle still running, skipping this tick');
            return;
          }
          reconcileRunning = true;
          try {
            const pool = getPgPool();
            const client = await pool.connect();
            try {
              const protoRoot = await loadProtoRoot(protoDir);
              await runReconcileCycle(client, rpc, protoRoot, {
                mode: periodicMode,
                maxLagBlocks: cfg.reconcileMaxLagBlocks ?? 10_000,
                fullBatchSize: cfg.reconcileFullBatchSize ?? 200,
                stateId: cfg.reconcileStateId ?? 'default',
              });
            } finally {
              client.release();
            }
          } catch (err) {
            log.error('[reconcile] loop error:', err instanceof Error ? err.message : String(err));
          } finally {
            reconcileRunning = false;
          }
        }, cfg.reconcileIntervalMs ?? 5 * 60 * 1000);
      }

      await followLoop(rpc, decodePool, sink, {
        startNext: endHeight + 1,
        pollMs,
        concurrency: cfg.concurrency,
        caseMode: cfg.caseMode,
        missingRetryIntervalMs: cfg.missingRetryIntervalMs,
      });
    }

    await cleanup(decodePool, sink, reconcileTimer);
  } catch (e) {
    const msg = e instanceof Error ? e.stack || e.message : String(e);
    log.error(msg);
    await gracefulCleanup(1);
  }
}

async function cleanup(decodePool: any, sink: any, reconcileTimer: NodeJS.Timeout | null = null) {
  log.info('Shutdown initiated…');
  try {
    if (reconcileTimer) {
      clearInterval(reconcileTimer);
    }
    if (decodePool) await decodePool.close();
    if (sink) {
      await sink.close?.(); // close() handles flushing internally
    }
    log.info('Shutdown complete.');
  } catch (err) {
    log.error('Shutdown error:', err);
  }
}

main();
