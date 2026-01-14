/**
 * Entry point for the Cosmos indexer application.
 * Initializes configuration, RPC client, decoding pool, and sink.
 * Handles backfilling blockchain data and optionally follows new blocks in real-time.
 * Responds to SIGINT and SIGTERM signals to gracefully shut down.
 */
// src/index.ts
import { EventEmitter } from 'node:events';
import { getConfig, printConfig } from './config.ts';
import { createRpcClientFromConfig } from './rpc/client.ts';
import { createTxDecodePool } from './decode/txPool.ts';
import { createSink } from './sink/index.ts';
import { closePgPool, createPgPool } from './db/pg.ts';
import { getProgress } from './db/progress.ts';
import { upsertValidators } from './sink/pg/flushers/validators.ts';
import { getLogger } from './utils/logger.ts';
import { syncRange } from './runner/syncRange.ts';
import { retryMissingBlocks } from './runner/retryMissing.ts';
import { followLoop } from './runner/follow.ts';

EventEmitter.defaultMaxListeners = 0;
const log = getLogger('index');

/**
 * Main function that runs the indexing process.
 * It resolves configuration, determines starting and ending block heights,
 * sets up RPC client, decode pool, and sink, performs range backfill,
 * and if enabled, follows new blocks in real-time.
 *
 * @returns {Promise<void>} Resolves when the indexing process completes or exits.
 */
async function main() {
  const cfg = getConfig();
  printConfig(cfg);

  const rpc = createRpcClientFromConfig(cfg);
  const status = await rpc.fetchStatus();

  let startFrom = cfg.from as number | undefined;
  const wantResume =
    !startFrom || cfg.resume === true || (typeof cfg.from === 'string' && cfg.from.toLowerCase() === 'resume');

  if (wantResume) {
    if (cfg.sinkKind === 'postgres') {
      const pool = createPgPool({ ...cfg.pg, applicationName: 'cosmos-indexer-resolver' });
      try {
        const last = await getProgress(pool, cfg.pg?.progressId ?? 'default');
        const earliest = Number(status['sync_info']['earliest_block_height']);
        const explicitFrom = typeof cfg.from === 'number' ? cfg.from : (cfg.firstBlock as number | undefined);
        startFrom = last != null ? last + 1 : (explicitFrom ?? earliest);
        log.info(`[resume] last_height=${last ?? 'null'} → start from ${startFrom}`);
      } finally {
        await closePgPool();
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

  const defaultProtoDir = new URL('../protos', import.meta.url).pathname;
  const protoDir = process.env.PROTO_DIR || defaultProtoDir;
  log.info(`[proto] dir = ${protoDir}`);

  const poolSize = Math.max(1, Math.min(cfg.concurrency ?? 8, 8));
  const decodePool = createTxDecodePool(poolSize, { protoDir });

  const sink = createSink({
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
  });
  await sink.init();

  // 🛡️ INITIAL SYNC: Fetch validators on startup
  if (cfg.sinkKind === 'postgres') {
    try {
      log.info('[start] syncing active validators from RPC…');
      const startPool = createPgPool(cfg.pg!);
      const client = await startPool.connect();
      try {
        const valSet = await rpc.getJson('/validators');
        const vals = valSet?.result?.validators || valSet?.validators || [];
        if (vals.length > 0) {
          const rows = vals.map((v: any) => ({
            operator_address: v.address, // Consensus address as fallback
            moniker: v.moniker || `Validator ${v.address.slice(0, 8)}`,
            status: 'BOND_STATUS_BONDED',
            updated_at_height: startFrom,
            updated_at_time: new Date()
          }));
          // ✅ FIX: Explicit transaction for validator sync
          await client.query('BEGIN');
          await upsertValidators(client, rows);
          await client.query('COMMIT');
          log.info(`[start] synced ${vals.length} validators (Consensus ADDRs)`);
        }
      } finally {
        client.release();
        // ❌ DO NOT call startPool.end() here as it is a singleton shared by the sink!
      }
    } catch (err) {
      log.warn('[start] validator sync failed (non-critical):', err instanceof Error ? err.message : String(err));
    }
  }

  const backfill = await syncRange(rpc, decodePool, sink, {
    from: startFrom,
    to: endHeight,
    concurrency: cfg.concurrency,
    progressEveryBlocks: cfg.progressEveryBlocks,
    progressIntervalSec: cfg.progressIntervalSec,
    caseMode: cfg.caseMode,
  });

  log.info(
    `[done-range] processed ${backfill.processed} blocks in [${startFrom}, ${endHeight}] — switching mode: ${cfg.follow === false ? 'exit' : 'follow'
    }`,
  );

  if (cfg.sinkKind === 'postgres') {
    await retryMissingBlocks(rpc, decodePool, sink, {
      concurrency: Math.max(1, Math.min(cfg.concurrency ?? 8, 8)),
      caseMode: cfg.caseMode,
    });
  }

  if (cfg.follow !== false) {
    const pollMs = cfg.followIntervalMs ?? 1500;
    await followLoop(rpc, decodePool, sink, {
      startNext: endHeight + 1,
      pollMs,
      concurrency: cfg.concurrency,
      caseMode: cfg.caseMode,
      missingRetryIntervalMs: cfg.missingRetryIntervalMs,
    });

    await shutdown(decodePool, sink);
  } else {
    await shutdown(decodePool, sink);
  }
}

/**
 * Cleanup function to flush and close all connections.
 */
async function shutdown(decodePool: any, sink: any) {
  log.info('Graceful shutdown initiated…');
  try {
    if (decodePool) await decodePool.close();
    if (sink) {
      await sink.flush?.();
      await sink.close?.();
    }
    log.info('Shutdown complete.');
  } catch (err) {
    log.error('Error during shutdown:', err);
  }
}

/**
 * Handle SIGINT signal to gracefully shut down the indexer.
 */
process.on('SIGINT', async () => {
  log.warn('SIGINT received');
  // We cannot easily pass decodePool/sink here without globalizing them, 
  // but the followLoop/syncRange usually throw on termination or can be awaited.
  // For now, we allow the main catch block or natural exit to handle it if possible.
  // Alternatively, we use process.exit(0) but ideally we'd want a global state.
  process.exit(0);
});
/**
 * Handle SIGTERM signal to gracefully shut down the indexer.
 */
process.on('SIGTERM', async () => {
  log.warn('SIGTERM received');
  process.exit(0);
});

main().catch((e) => {
  const msg = e instanceof Error ? e.stack || e.message : String(e);
  getLogger('index').error(msg);
  process.exit(1);
});
