/**
 * PostgreSQL sink implementation.
 * Updated for Zigchain Custom Modules (DEX, Liquidity, WASM) AND Governance.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Sink, SinkConfig } from './types.js';
import { createPgPool, getPgPool, closePgPool } from '../db/pg.js';
import { ensureCorePartitions, ensureIbcPartitions } from '../db/partitions.js';
import { upsertProgress } from '../db/progress.js';
import { execBatchedInsert } from './pg/batch.js';
import { recordMissingBlock, resolveMissingBlock } from '../db/missing_blocks.js';
import { getLogger } from '../utils/logger.js';
import {
  pickMessages,
  pickLogs,
  attrsToPairs,
  toNum,
  buildFeeFromDecodedFee,
  collectSignersFromMessages,
  pickSigner,
  parseCoin,
  parseCoins, // ✅ ADDED
  findAttr,
  normArray,
  parseDec,
  tryParseJson,
  toBigIntStr,
  toDateFromTimestamp,
  decodeHexToJson // ✅ ADDED: For IBC packet_data_hex decoding
} from './pg/parsing.js';

// Standard Flushers
import { flushBlocks } from './pg/flushers/blocks.js';
import { flushTxs } from './pg/flushers/txs.js';
import { flushMsgs } from './pg/flushers/msgs.js';
import { flushEvents } from './pg/flushers/events.js';
import { flushAttrs } from './pg/flushers/attrs.js';
import { flushTransfers } from './pg/flushers/transfers.js';
import { flushStakeDeleg } from './pg/flushers/stake_deleg.js';
import { flushStakeDistr } from './pg/flushers/stake_distr.js';
import { flushWasmExec } from './pg/flushers/wasm_exec.js';
import { flushWasmEvents } from './pg/flushers/wasm_events.js';
import { flushWasmEventAttrs } from './pg/flushers/wasm_event_attrs.js';
// ✅ ADDED: Gov Flushers
import { flushGovDeposits, flushGovVotes, upsertGovProposals } from './pg/flushers/gov.js';
// ✅ ADDED: Validator Flusher
import { upsertValidators } from './pg/flushers/validators.js';
// ✅ ADDED: IBC Flusher
import { flushIbcPackets } from './pg/flushers/ibc_packets.js';
import { flushIbcTransfers } from './pg/flushers/ibc_transfers.js';
import { flushIbcChannels } from './pg/flushers/ibc_channels.js';
import { flushIbcClients } from './pg/flushers/ibc_clients.js';
import { flushIbcDenoms } from './pg/flushers/ibc_denoms.js';
import { flushIbcConnections } from './pg/flushers/ibc_connections.js';
import { flushAuthzGrants } from './pg/flushers/authz_grants.js';
import { flushFeeGrants } from './pg/flushers/fee_grants.js';
import { flushCw20Transfers } from './pg/flushers/cw20_transfers.js';

// ✅ ADDED: Bank & Params Flushers
import { flushBalanceDeltas } from './pg/flushers/bank.js';
import { flushNetworkParams } from './pg/flushers/params.js';
// ✅ ADDED: WASM Registry Flusher
import { flushWasmRegistry } from './pg/flushers/wasm.js';
import { flushWasmAdminChanges } from './pg/flushers/wasm_admin_changes.js';

// ✅ Zigchain Flusher
import { flushZigchainData } from './pg/flushers/zigchain.js';

// ✅ WASM DEX Swap Analytics
import { flushWasmSwaps, flushFactoryTokens } from './pg/flushers/wasm_swaps.js';

// ✅ Specialized WASM Analytics
import { flushWasmAnalytics } from './pg/flushers/wasm_analytics.js';

// ✅ Unknown Messages Quarantine
import { flushUnknownMessages } from './pg/flushers/unknown_msgs.js';

// ✅ Zigchain Factory Supply Tracking
import { flushFactorySupplyEvents } from './pg/flushers/factory_supply.js';

const log = getLogger('sink/postgres');

export type PostgresMode = 'block-atomic' | 'batch-insert';

function extractExpiration(val: any): Date | null {
  const direct = toDateFromTimestamp(val);
  if (direct) return direct;
  const nested = val?.expiration ?? val?.basic?.expiration ?? val?.periodic?.expiration ?? val?.allowance?.expiration;
  return toDateFromTimestamp(nested);
}

export interface PostgresSinkConfig extends SinkConfig {
  pg: {
    connectionString?: string;
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    database?: string;
    ssl?: boolean;
    poolSize?: number;
    progressId?: string;
  };
  /**
   * RPC URL needed for Reconciliation Engine
   */
  rpcUrl: string;
  mode?: PostgresMode;
  batchSizes?: {
    blocks?: number;
    txs?: number;
    msgs?: number;
    events?: number;
    attrs?: number;
  };
}

type BlockLine = any;

export class PostgresSink implements Sink {
  private cfg: PostgresSinkConfig;
  private mode: PostgresMode;

  // Standard Buffers
  private bufBlocks: any[] = [];
  private bufTxs: any[] = [];
  private bufMsgs: any[] = [];
  private bufEvents: any[] = [];
  private bufAttrs: any[] = [];
  private bufTransfers: any[] = [];
  private bufStakeDeleg: any[] = [];
  private bufStakeDistr: any[] = [];
  private bufWasmExec: any[] = [];
  private bufWasmEvents: any[] = [];
  private bufWasmEventAttrs: any[] = [];

  // ✅ Gov Buffers (Now Used)
  private bufGovDeposits: any[] = [];
  private bufGovVotes: any[] = [];
  private bufGovProposals: any[] = [];

  // ✅ Validator Buffer
  private bufValidators: any[] = [];
  private bufValidatorSet: any[] = [];
  private bufMissedBlocks: any[] = [];

  // ✅ IBC Buffer
  private bufIbcPackets: any[] = [];
  private bufIbcChannels: any[] = [];
  private bufIbcTransfers: any[] = [];
  private bufIbcClients: any[] = [];
  private bufIbcDenoms: any[] = [];
  private bufIbcConnections: any[] = [];

  // ✅ Authz/Feegrant Buffer
  private bufAuthzGrants: any[] = [];
  private bufFeeGrants: any[] = [];

  // ✅ ADDED: Missing Buffers
  private bufBalanceDeltas: any[] = [];
  private bufWasmCodes: any[] = [];
  private bufWasmContracts: any[] = [];
  private bufWasmMigrations: any[] = [];
  private bufWasmAdminChanges: any[] = [];
  private bufWasmInstantiateConfigs: any[] = [];
  private bufNetworkParams: any[] = [];

  // Zigchain Buffers
  private bufFactoryDenoms: any[] = [];
  private bufDexPools: any[] = [];
  private bufDexSwaps: any[] = [];
  private bufDexLiquidity: any[] = [];
  private bufWrapperSettings: any[] = [];
  private bufCw20Transfers: any[] = [];

  // ✅ WASM DEX Swap Analytics
  private bufWasmSwaps: any[] = [];
  private bufFactoryTokens: any[] = [];
  private bufWrapperEvents: any[] = [];

  // ✅ Specialized WASM Analytics
  private bufWasmOracleUpdates: any[] = [];
  private bufWasmTokenEvents: any[] = [];

  // ✅ Unknown Messages Quarantine
  private bufUnknownMsgs: any[] = [];

  // ✅ Zigchain Factory Supply Tracking
  private bufFactorySupplyEvents: any[] = [];

  private batchSizes = {
    blocks: 1000,
    txs: 2000,
    msgs: 5000,
    events: 5000,
    attrs: 10000,
    transfers: 5000,
    stakeDeleg: 5000,
    stakeDistr: 5000,
    wasmExec: 5000,
    wasmEvents: 5000,
    wasmEventAttrs: 5000,
    govDeposits: 5000,
    govVotes: 5000,
    govProposals: 1000,
    validators: 500,
    ibcPackets: 2000,
    ibcChannels: 500,

    ibcTransfers: 2000,
    zigchain: 2000,
    cw20Transfers: 5000
  };


  constructor(cfg: PostgresSinkConfig) {
    this.cfg = cfg;
    this.mode = cfg.mode ?? 'batch-insert';
    if (cfg.batchSizes) Object.assign(this.batchSizes, cfg.batchSizes);
  }

  async init(): Promise<void> {
    await createPgPool({ ...this.cfg.pg, applicationName: 'cosmos-indexer' });
  }

  async write(line: any): Promise<void> {
    let obj: BlockLine;
    if (typeof line === 'string') {
      try {
        obj = JSON.parse(line);
      } catch {
        return;
      }
    } else {
      obj = line;
    }
    if (obj?.error) return;

    if (this.mode === 'block-atomic') {
      await this.persistBlockAtomic(obj);
    } else {
      await this.persistBlockBuffered(obj);
    }
  }

  async flush(): Promise<void> {
    if (this.mode === 'batch-insert') {
      await this.flushAll();
    }
  }

  async close(): Promise<void> {
    await this.flush?.();
    await closePgPool();
  }

  async recordMissingBlock(height: number, error: string | null): Promise<void> {
    const pool = getPgPool();
    const client = await pool.connect();
    try {
      await recordMissingBlock(client, height, error);
    } finally {
      client.release();
    }
  }

  async resolveMissingBlock(height: number): Promise<void> {
    const pool = getPgPool();
    const client = await pool.connect();
    try {
      await resolveMissingBlock(client, height);
    } finally {
      client.release();
    }
  }

  private extractRows(blockLine: BlockLine) {
    const height = Number(blockLine?.meta?.height);
    const time = new Date(blockLine?.meta?.time);

    // Block Extraction
    const b = blockLine.block;
    const blockRow = {
      height,
      block_hash: b?.block_id?.hash ?? null,
      time,
      proposer_address: b?.block?.header?.proposer_address ?? null,
      tx_count: Array.isArray(blockLine?.txs) ? blockLine.txs.length : 0,
      last_commit_hash: b?.block?.header?.last_commit_hash ?? b?.block?.last_commit?.block_id?.hash ?? null,
      data_hash: b?.block?.header?.data_hash ?? null,
      evidence_count: Array.isArray(b?.block?.evidence?.evidence) ? b.block.evidence.evidence.length : 0,
      app_hash: b?.block?.header?.app_hash ?? null,
    };

    // ✅ Validator Set & Missed Blocks Extraction
    const validatorSetRows: any[] = [];
    const missedBlocksRows: any[] = [];

    // ✅ Define ALL row arrays at the top to fix scope issues
    const txRows: any[] = [];
    const msgRows: any[] = [];
    const evRows: any[] = [];
    const attrRows: any[] = [];
    const transfersRows: any[] = [];

    // Zigchain
    const factoryDenomsRows: any[] = [];
    const dexPoolsRows: any[] = [];
    const dexSwapsRows: any[] = [];
    const dexLiquidityRows: any[] = [];
    const wrapperSettingsRows: any[] = [];

    // WASM
    const wasmExecRows: any[] = [];
    const wasmEventsRows: any[] = [];
    const wasmEventAttrsRows: any[] = [];

    // ✅ Gov
    const govVotesRows: any[] = [];
    const govDepositsRows: any[] = [];
    const govProposalsRows: any[] = [];

    // ✅ Validator
    const validatorsRows: any[] = [];

    // ✅ IBC
    const ibcPacketsRows: any[] = [];
    const ibcChannelsRows: any[] = [];
    const ibcTransfersRows: any[] = [];
    const ibcClientsRows: any[] = [];
    const ibcDenomsRows: any[] = [];
    const ibcConnectionsRows: any[] = [];

    // ✅ Authz / Feegrant
    const authzGrantsRows: any[] = [];
    const feeGrantsRows: any[] = [];

    // ✅ Tokens (CW20)
    const cw20TransfersRows: any[] = [];

    // ✅ Staking & Distribution
    const stakeDelegRows: any[] = [];
    const stakeDistrRows: any[] = [];

    // ✅ ADDED: New Local Arrays
    const balanceDeltasRows: any[] = [];
    const wasmCodesRows: any[] = [];
    const wasmContractsRows: any[] = [];
    const wasmMigrationsRows: any[] = [];
    const wasmAdminChangesRows: any[] = [];
    const wasmInstantiateConfigsRows: any[] = [];
    const networkParamsRows: any[] = [];

    // ✅ WASM DEX Swaps Analytics
    const wasmSwapsRows: any[] = [];
    const factoryTokensRows: any[] = [];

    // ✅ Unknown Messages Quarantine
    const unknownMsgsRows: any[] = [];
    const factorySupplyEventsRows: any[] = [];
    const wrapperEventsRows: any[] = [];
    const wasmOracleUpdatesRows: any[] = [];
    const wasmTokenEventsRows: any[] = [];

    // 🟢 BANK BALANCE DELTAS (Helper)
    const extractBalanceDeltas = (evType: string, attrsPairs: { key: string, value: string | null }[], tx_hash?: string, msg_index?: number, event_index?: number) => {
      // ✅ FIX: Support multiple attribute keys
      const getReceiver = () =>
        findAttr(attrsPairs, 'receiver') ||
        findAttr(attrsPairs, 'recipient') ||
        findAttr(attrsPairs, 'to_address') ||
        findAttr(attrsPairs, 'to');

      const getSpender = () =>
        findAttr(attrsPairs, 'spender') ||
        findAttr(attrsPairs, 'sender') ||
        findAttr(attrsPairs, 'from_address') ||
        findAttr(attrsPairs, 'from');

      if (evType === 'coin_received' || evType === 'coin_spent') {
        const acc = evType === 'coin_received' ? getReceiver() : getSpender();

        // ✅ FIX: Strict Null Guard - Critical for preventing corrupt aggregates
        if (!acc || acc.trim() === '') {
          return;
        }

        const amountStr = findAttr(attrsPairs, 'amount');
        const coins = parseCoins(amountStr);
        for (const coin of coins) {
          balanceDeltasRows.push({
            height,
            account: acc,
            denom: coin.denom,
            delta: evType === 'coin_received' ? coin.amount : `-${coin.amount}`,
            // ✅ ADDED: Unique Constraint Keys for Intra-Block Deduplication
            tx_hash: (typeof tx_hash !== 'undefined') ? tx_hash : 'block_event',
            msg_index: (typeof msg_index !== 'undefined') ? msg_index : -1,
            event_index: (typeof event_index !== 'undefined') ? event_index : -1 // ✅ FIX: Use param, not 'ei'
          });
        }
      }
    };

    const vSet = blockLine.validator_set;
    if (vSet?.validators) {
      for (const v of vSet.validators as any[]) {
        validatorSetRows.push({
          height,
          operator_address: v.address,
          voting_power: v.voting_power,
          proposer_priority: v.proposer_priority
        });
      }
    }

    // ✅ GOVERNANCE: Process EndBlock Events for Timestamps
    // Timestamps like voting_end_time are often only found in EndBlock events, not in transactions.
    const endEvents = blockLine.block_results?.end_block_events ?? [];
    for (const ev of endEvents) {
      if (ev.type === 'active_proposal' || ev.type === 'proposal_deposit' || ev.type === 'proposal_vote' || ev.type === 'inactive_proposal') {
        const attrs = attrsToPairs(ev.attributes);
        const pid = findAttr(attrs, 'proposal_id');
        if (pid) {
          const votingStart = toDateFromTimestamp(findAttr(attrs, 'voting_period_start') || findAttr(attrs, 'voting_start_time'));
          const votingEnd = toDateFromTimestamp(findAttr(attrs, 'voting_period_end') || findAttr(attrs, 'voting_end_time'));
          const depositEnd = toDateFromTimestamp(findAttr(attrs, 'deposit_end_time'));

          if (votingStart || votingEnd || depositEnd) {
            govProposalsRows.push({
              proposal_id: pid,
              // If we see voting timestamps, it's definitely in voting period or passed it
              status: (votingEnd && votingEnd < time) ? 'passed' : (votingStart ? 'voting_period' : undefined),
              voting_start: votingStart,
              voting_end: votingEnd,
              deposit_end: depositEnd
            } as any);
          }
        }
      }
    }

    // Missed blocks: look at current block signatures for height-1
    const signatures = b?.block?.last_commit?.signatures;
    if (Array.isArray(signatures)) {
      for (const sig of signatures) {
        // block_id_flag: 1 = BLOCK_ID_FLAG_ABSENT, 2 = COMMIT, 3 = NIL
        const isAbsent = sig.block_id_flag === 1 || sig.block_id_flag === 'BLOCK_ID_FLAG_ABSENT';
        if (isAbsent && sig.validator_address) {
          missedBlocksRows.push({
            operator_address: sig.validator_address,
            height: height - 1
          });
          log.debug(`[uptime] missed block: ${sig.validator_address} at height ${height - 1} `);
        }
      }
    }


    const txs = Array.isArray(blockLine?.txs) ? blockLine.txs : [];

    for (const tx of txs) {
      const tx_hash = tx.hash ?? tx.txhash ?? tx.tx_hash ?? null;
      const txIbcIntents: any[] = [];
      const tx_index = Number(tx.index ?? tx.tx_index ?? tx?.tx_response?.index ?? 0);
      const code = Number(tx.code ?? tx?.tx_response?.code ?? 0);
      const gas_wanted = toNum(tx.gas_wanted ?? tx?.tx_response?.gas_wanted);
      const gas_used = toNum(tx.gas_used ?? tx?.tx_response?.gas_used);
      const fee = tx.fee ?? buildFeeFromDecodedFee(tx?.decoded?.auth_info?.fee);
      const memo = tx.memo ?? tx?.decoded?.body?.memo ?? null;
      let signers: string[] | null = Array.isArray(tx.signers) ? tx.signers : null;
      const raw_tx = tx.raw_tx ?? tx?.decoded ?? tx?.raw ?? null;
      const log_summary = tx.log_summary ?? tx?.tx_response?.raw_log ?? null;

      const msgs = pickMessages(tx);
      if (!signers || signers.length === 0) {
        const derived = collectSignersFromMessages(msgs);
        if (derived) signers = derived;
      }
      const firstSigner = Array.isArray(signers) && signers.length ? signers[0] : null;

      txRows.push({
        tx_hash, height, tx_index, code, gas_wanted, gas_used, fee, memo, signers, raw_tx, log_summary, time
      });

      const logs = pickLogs(tx);
      // 🚀 PERFORMANCE: Map-based log lookup (O(1) instead of O(N))
      const logMap = new Map<number, any>();
      for (const l of logs) {
        logMap.set(Number(l.msg_index), l);
      }

      // --- PROCESS MESSAGES ---
      for (let i = 0; i < msgs.length; i++) {
        const m = msgs[i];
        const type = m?.['@type'] ?? m?.type_url ?? '';

        // ✅ Detect unknown/undecoded messages (quarantine)
        const isUnknown = m?.['_raw'] || m?.['value_b64'];
        if (isUnknown) {
          unknownMsgsRows.push({
            tx_hash,
            msg_index: i,
            height,
            type_url: type,
            raw_value: m?.['_raw'] || m?.['value_b64'],
            signer: null  // Can't decode signer from unknown msg
          });
          log.warn(`[quarantine] Unknown message type: ${type} at height ${height} `);
          continue;  // Skip domain table processing for this message
        }

        msgRows.push({
          tx_hash, msg_index: i, height, type_url: type, value: m,
          signer: pickSigner(m),
        });

        const msgLog = logMap.get(i) || logMap.get(-1); // Fallback to flat log if per-msg missing

        // 🟢 GOVERNANCE LOGIC (INCLUDED FAILED TXS) 🟢

        // 1. VOTE (Simple and Weighted)
        if (type === '/cosmos.gov.v1beta1.MsgVote' || type === '/cosmos.gov.v1.MsgVote') {
          govVotesRows.push({
            proposal_id: m.proposal_id,
            voter: m.voter,
            option: m.option?.toString() ?? 'VOTE_OPTION_UNSPECIFIED',
            weight: null, // Simple votes have no weight
            height,
            tx_hash
          });
        }

        // 1b. WEIGHTED VOTE
        if (type === '/cosmos.gov.v1beta1.MsgVoteWeighted' || type === '/cosmos.gov.v1.MsgVoteWeighted') {
          const options = Array.isArray(m.options) ? m.options : [];
          for (const opt of options) {
            govVotesRows.push({
              proposal_id: m.proposal_id,
              voter: m.voter || m.signer || firstSigner,
              option: opt.option?.toString() ?? 'VOTE_OPTION_UNSPECIFIED',
              weight: opt.weight ?? '1.0', // Weight is a decimal string like "0.5"
              height,
              tx_hash
            });
          }
          // ✅ ENHANCEMENT: Any vote implies the proposal is in voting period
          if (m.proposal_id) {
            govProposalsRows.push({
              proposal_id: m.proposal_id,
              status: 'voting_period'
            } as any);
          }
        }

        // 2. DEPOSIT
        if (type === '/cosmos.gov.v1beta1.MsgDeposit' || type === '/cosmos.gov.v1.MsgDeposit') {
          const amounts = Array.isArray(m.amount) ? m.amount : [m.amount];
          for (const coin of amounts) {
            if (!coin) continue;
            govDepositsRows.push({
              proposal_id: m.proposal_id,
              depositor: m.depositor || m.signer || firstSigner,
              amount: coin.amount,
              denom: coin.denom,
              height,
              tx_hash
            });
          }

          // ✅ ENHANCEMENT: Check if deposit triggers voting period
          const activeEv = msgLog?.events.find((e: any) => e.type === 'proposal_deposit' || e.type === 'active_proposal');
          const vsAt = findAttr(attrsToPairs(activeEv?.attributes), 'voting_period_start');
          if (vsAt && m.proposal_id) {
            govProposalsRows.push({
              proposal_id: m.proposal_id,
              status: 'voting_period',
              voting_start: toDateFromTimestamp(vsAt),
            } as any);
          }
        }

        // 3. PROPOSAL (Only Success - ID is generated on-chain)
        if ((type === '/cosmos.gov.v1beta1.MsgSubmitProposal' || type === '/cosmos.gov.v1.MsgSubmitProposal') && code === 0) {
          const event = msgLog?.events.find((e: any) => e.type === 'submit_proposal');
          const pid = findAttr(attrsToPairs(event?.attributes), 'proposal_id');

          if (pid) {
            const submitter = m.proposer || m.signer || firstSigner;
            const proposalType = m.content?.['@type'] ??
              m.content?.type_url ??
              (Array.isArray(m.messages) && (m.messages[0]?.['@type'] ?? m.messages[0]?.type_url)) ??
              findAttr(attrsToPairs(event?.attributes), 'proposal_type') ??
              findAttr(attrsToPairs(event?.attributes), 'proposal_messages')?.split(',').filter(Boolean).pop() ??
              null;

            // ✅ EXTRACTION: Initial Deposit
            const initialDeposit = m.initial_deposit ?? m.initialDeposit ?? null;

            // ✅ EXTRACTION: Changes (for Param Changes or legacy content)
            const content = m.content || (Array.isArray(m.messages) ? m.messages[0] : null);
            const changes = content?.params ?? content?.changes ?? content?.plan ?? content?.msg ?? content?.msgs ?? null;

            // ✅ EXTRACTION: Timestamps from events (accurate vs block time)
            const attrs = attrsToPairs(event?.attributes);
            const depositEnd = toDateFromTimestamp(findAttr(attrs, 'deposit_end_time'));
            const votingStart = toDateFromTimestamp(findAttr(attrs, 'voting_period_start') || findAttr(attrs, 'voting_start_time'));
            const votingEnd = toDateFromTimestamp(findAttr(attrs, 'voting_period_end') || findAttr(attrs, 'voting_end_time'));

            govProposalsRows.push({
              proposal_id: pid,
              submitter,
              title: m.content?.title ?? m.title ?? content?.title ?? '',
              summary: m.content?.description ?? m.summary ?? content?.description ?? '',
              proposal_type: proposalType,
              submit_time: time,
              status: votingStart ? 'voting_period' : 'deposit_period',
              deposit_end: depositEnd,
              voting_start: votingStart,
              voting_end: votingEnd,
              total_deposit: initialDeposit,
              changes: changes,
            });
          }
        }

        // 🟢 AUTHZ / FEEGRANT (SUCCESS ONLY) 🟢
        if (
          code === 0 &&
          (type === '/cosmos.authz.v1beta1.MsgGrant' || type === '/cosmos.authz.v1.MsgGrant')
        ) {
          const grant = m?.grant ?? {};
          const auth = grant?.authorization ?? m?.authorization ?? null;
          const msgTypeUrl = auth?.msg ?? auth?.msg_type_url ?? auth?.['@type'] ?? auth?.type_url ?? null;
          if (m?.granter && m?.grantee && msgTypeUrl) {
            authzGrantsRows.push({
              granter: m.granter,
              grantee: m.grantee,
              msg_type_url: msgTypeUrl,
              expiration: extractExpiration(grant),
              height,
              revoked: false,
            });
          }
        }

        if (
          code === 0 &&
          (type === '/cosmos.authz.v1beta1.MsgRevoke' || type === '/cosmos.authz.v1.MsgRevoke')
        ) {
          if (m?.granter && m?.grantee && m?.msg_type_url) {
            authzGrantsRows.push({
              granter: m.granter,
              grantee: m.grantee,
              msg_type_url: m.msg_type_url,
              expiration: null,
              height,
              revoked: true,
            });
          }
        }

        if (
          code === 0 &&
          (type === '/cosmos.feegrant.v1beta1.MsgGrantAllowance' || type === '/cosmos.feegrant.v1.MsgGrantAllowance')
        ) {
          const allowance = m?.allowance ?? null;
          if (m?.granter && m?.grantee) {
            feeGrantsRows.push({
              granter: m.granter,
              grantee: m.grantee,
              allowance,
              expiration: extractExpiration(allowance),
              height,
              revoked: false,
            });
          }
        }

        if (
          code === 0 &&
          (type === '/cosmos.feegrant.v1beta1.MsgRevokeAllowance' ||
            type === '/cosmos.feegrant.v1.MsgRevokeAllowance')
        ) {
          if (m?.granter && m?.grantee) {
            feeGrantsRows.push({
              granter: m.granter,
              grantee: m.grantee,
              allowance: null,
              expiration: null,
              height,
              revoked: true,
            });
          }
        }

        // 🟢 WASM REGISTRY (STORE/INSTANTIATE) 🟢
        if (type.endsWith('.MsgStoreCode') ||
          type.endsWith('.MsgStoreAndInstantiateContract') ||
          type.endsWith('.MsgStoreAndMigrateContract')) {
          const event = msgLog?.events.find((e: any) => e.type === 'store_code');
          const attrs = attrsToPairs(event?.attributes);
          const codeId = findAttr(attrs, 'code_id');
          const checksum = findAttr(attrs, 'code_checksum') || findAttr(attrs, 'checksum') || m.checksum;

          if (codeId) {
            // Try to get instantiate_permission from message first, then fall back to event
            let instantiatePermission = m.instantiate_permission || m.instantiatePermission ||
              m.instantiate_config || m.instantiateConfig;

            // Fallback: extract from store_code event if not in message
            if (!instantiatePermission) {
              const permissionStr = findAttr(attrs, 'instantiate_permission') ||
                findAttr(attrs, 'access_config') ||
                findAttr(attrs, 'instantiate_config');
              if (permissionStr) {
                instantiatePermission = tryParseJson(permissionStr);
              }
            }

            wasmCodesRows.push({
              code_id: codeId,
              checksum: checksum || '', // Ensure not null
              creator: m.sender || m.authority || m.signer || firstSigner,
              instantiate_permission: instantiatePermission,
              store_tx_hash: tx_hash,
              store_height: height
            });
            if (!instantiatePermission) {
              log.debug(`[wasm - store] instantiate_permission is missing in ${tx_hash}. Keys present: ${Object.keys(m).join(', ')} `);
            }
          }

          // If it's a "StoreAndInstantiate", we also capture the contract creation if it succeeded
          if (type.endsWith('.MsgStoreAndInstantiateContract') && code === 0) {
            const instEvent = msgLog?.events.find((e: any) => e.type === 'instantiate');
            const addr = findAttr(attrsToPairs(instEvent?.attributes), '_contract_address') || findAttr(attrsToPairs(instEvent?.attributes), 'contract_address');
            if (addr) {
              wasmContractsRows.push({
                address: addr,
                code_id: codeId,
                creator: m.authority || m.sender || firstSigner,
                admin: m.admin,
                label: m.label,
                created_height: height,
                created_tx_hash: tx_hash
              });
            }
          }
        }

        if (type.endsWith('.MsgUpdateInstantiateConfig') && code === 0) {
          wasmInstantiateConfigsRows.push({
            code_id: m.code_id,
            instantiate_permission: m.new_instantiate_permission || m.newInstantiatePermission || m.instantiate_permission || m.instantiatePermission,
            height,
            tx_hash
          });
        }

        if (type.endsWith('.MsgInstantiateContract') ||
          type.endsWith('.MsgInstantiateContract2')) {
          const event = msgLog?.events.find((e: any) => e.type === 'instantiate');
          const addr = findAttr(attrsToPairs(event?.attributes), '_contract_address') || findAttr(attrsToPairs(event?.attributes), 'contract_address');
          if (addr) {
            wasmContractsRows.push({
              address: addr,
              code_id: m.code_id,
              creator: m.sender,
              admin: m.admin,
              label: m.label,
              created_height: height,
              created_tx_hash: tx_hash
            });
          }
        }

        if (type.endsWith('.MsgMigrateContract')) {
          // Extract from_code_id from migrate event (contains old code_id)
          const migrateEvent = msgLog?.events.find((e: any) => e.type === 'migrate');
          const migrateAttrs = attrsToPairs(migrateEvent?.attributes);
          const fromCodeId = findAttr(migrateAttrs, 'code_id') || findAttr(migrateAttrs, 'old_code_id');

          wasmMigrationsRows.push({
            contract: m.contract,
            from_code_id: fromCodeId ? toBigIntStr(fromCodeId) : null,
            to_code_id: m.code_id,
            height,
            tx_hash
          });
        }

        // 🟢 ZIGCHAIN LOGIC 🟢
        if (type.endsWith('.MsgCreateDenom')) {
          const event = msgLog?.events.find((e: any) => e.type === 'create_denom');
          let finalDenom = event ? findAttr(attrsToPairs(event.attributes), 'denom') : `factory / ${m.creator}/${m.sub_denom}`;

          factoryDenomsRows.push({
            denom: finalDenom,
            creator_address: m.creator,
            sub_denom: m.sub_denom,
            minting_cap: m.minting_cap,
            uri: m.URI,
            uri_hash: m.URI_hash,
            description: m.description,
            creation_tx_hash: tx_hash,
            block_height: height
          });
        }

        if (type.endsWith('.MsgMintAndSendTokens') || type.endsWith('.MsgBurnTokens')) {
          const denom = m.token?.denom || m.amount?.denom || m.denom;
          if (denom) {
            factorySupplyEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              denom,
              action: type.endsWith('.MsgMintAndSendTokens') ? 'mint' : 'burn',
              amount: m.token?.amount || m.amount?.amount || m.amount || null,
              sender: m.signer || m.sender || m.creator,
              recipient: m.recipient || null,
              metadata: null
            });
          }
        }

        if (type.endsWith('.MsgSetDenomMetadata')) {
          if (m.metadata?.base) {
            factorySupplyEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              denom: m.metadata.base,
              action: 'set_metadata',
              amount: null,
              sender: m.signer || m.sender || m.creator,
              recipient: null,
              metadata: m.metadata
            });
          }
        }

        if (type.endsWith('.MsgFundModuleWallet') || type.endsWith('.MsgWithdrawFromModuleWallet')) {
          const coins = Array.isArray(m.amount) ? m.amount : (m.amount ? [m.amount] : []);
          for (const coin of coins) {
            wrapperEventsRows.push({
              height,
              tx_hash,
              msg_index: i,
              sender: m.signer || m.sender,
              action: type.endsWith('.MsgFundModuleWallet') ? 'fund_module' : 'withdraw_module',
              amount: coin?.amount || null,
              denom: coin?.denom || null,
              metadata: null
            });
          }
        }

        if (type.endsWith('.MsgUpdateIbcSettings')) {
          wrapperEventsRows.push({
            height,
            tx_hash,
            msg_index: i,
            sender: m.sender,
            action: 'update_ibc_settings',
            amount: null,
            denom: null,
            metadata: {
              native_client_id: m.native_client_id,
              counterparty_client_id: m.counterparty_client_id,
              native_port: m.native_port,
              counterparty_port: m.counterparty_port,
              native_channel: m.native_channel,
              counterparty_channel: m.counterparty_channel
            }
          });
        }

        if (type.endsWith('.MsgCreatePool')) {
          let poolId = null;
          let lpToken = null;
          let pairId = null;
          let baseReserve = null;
          let quoteReserve = null;

          if (msgLog) {
            for (const e of msgLog.events) {
              const pairs = attrsToPairs(e.attributes);
              const pid = findAttr(pairs, 'pool_id');
              if (pid) poolId = pid;
              const lpt = findAttr(pairs, 'lp_token') || findAttr(pairs, 'lptoken_denom');
              if (lpt) lpToken = lpt;
              // ✅ Extract pair_id if present
              const pId = findAttr(pairs, 'pair_id');
              if (pId) pairId = pId;
            }
          }

          // Initial reserves from the message (create pool deposits initial liquidity)
          baseReserve = m.base?.amount || '0';
          quoteReserve = m.quote?.amount || '0';

          if (poolId) {
            dexPoolsRows.push({
              pool_id: poolId,
              creator_address: m.creator,
              pair_id: pairId,
              base_denom: m.base?.denom,
              quote_denom: m.quote?.denom,
              lp_token_denom: lpToken,
              base_reserve: baseReserve,
              quote_reserve: quoteReserve,
              block_height: height,
              tx_hash: tx_hash
            });
          }
        }

        if (type.endsWith('.MsgSwapExactIn') || type.endsWith('.MsgSwapExactOut') || type.endsWith('.MsgSwap')) {
          let priceImpact = null;
          // Try to extract price_impact from event or calculate from amounts
          if (msgLog) {
            for (const e of msgLog.events) {
              const pairs = attrsToPairs(e.attributes);
              const pi = findAttr(pairs, 'price_impact');
              if (pi) {
                priceImpact = pi;
                break;
              }
            }
          }

          dexSwapsRows.push({
            tx_hash,
            msg_index: i,
            pool_id: m.pool_id,
            sender_address: m.signer || m.creator || firstSigner,
            token_in_denom: m.incoming?.denom || m.incoming_max?.denom,
            token_in_amount: m.incoming?.amount || m.incoming_max?.amount,
            token_out_denom: m.outgoing?.denom || m.outgoing_min?.denom,
            token_out_amount: m.outgoing?.amount || m.outgoing_min?.amount,
            price_impact: priceImpact,
            block_height: height
          });
        }

        if (type.endsWith('.MsgAddLiquidity') || type.endsWith('.MsgRemoveLiquidity')) {
          let poolId = m.pool_id;
          if (!poolId && msgLog) {
            for (const e of msgLog.events) {
              const pid = findAttr(attrsToPairs(e.attributes), 'pool_id');
              if (pid) {
                poolId = pid;
                break;
              }
            }
          }

          if (poolId) {
            dexLiquidityRows.push({
              tx_hash,
              msg_index: i,
              pool_id: poolId,
              sender_address: m.creator || m.signer || firstSigner,
              action_type: type.includes('Add') ? 'ADD' : 'REMOVE',
              amount_0: m.base?.amount || m.token_0_amount || m.max_token_0,
              amount_1: m.quote?.amount || m.token_1_amount || m.max_token_1,
              shares_minted_burned: m.lptoken?.amount || m.share_amount || m.lp_token_out,
              block_height: height
            });
          }
        }

        if (type.endsWith('.MsgUpdateIbcSettings')) {
          wrapperSettingsRows.push({
            denom: m.denom,
            native_client_id: m.native_client_id,
            counterparty_client_id: m.counterparty_client_id,
            native_port: m.native_port,
            counterparty_port: m.counterparty_port,
            native_channel: m.native_channel,
            counterparty_channel: m.counterparty_channel,
            decimal_difference: m.decimal_difference,
            updated_at_height: height
          });
        }

        // 🟢 WASM LOGIC 🟢
        if (type.endsWith('.MsgExecuteContract')) {
          wasmExecRows.push({
            tx_hash, msg_index: i, contract: m?.contract ?? m?.contract_address, caller: m?.sender,
            funds: m?.funds, msg: tryParseJson(m?.msg), success: code === 0, error: code === 0 ? null : (log_summary),
            gas_used, height
          });
        }

        // 🟢 WASM ADMIN CHANGES (Security Auditing) 🟢
        if (type.endsWith('.MsgUpdateAdmin') && code === 0) {
          wasmAdminChangesRows.push({
            contract: m.contract,
            height,
            tx_hash,
            msg_index: i,
            old_admin: null, // Would need state query to get old admin
            new_admin: m.new_admin,
            action: 'update'
          });
        }

        if (type.endsWith('.MsgClearAdmin') && code === 0) {
          wasmAdminChangesRows.push({
            contract: m.contract,
            height,
            tx_hash,
            msg_index: i,
            old_admin: null, // Would need state query to get old admin
            new_admin: null,
            action: 'clear'
          });
        }

        // 🟢 WASM SUDO (Governance-initiated execution) 🟢
        if (type.endsWith('.MsgSudoContract')) {
          wasmExecRows.push({
            tx_hash, msg_index: i, contract: m?.contract ?? m?.contract_address,
            caller: 'governance', // Sudo is always governance-initiated
            funds: null, msg: tryParseJson(m?.msg), success: code === 0,
            error: code === 0 ? null : (log_summary),
            gas_used, height
          });
        }

        // 🟢 STAKING LOGIC 🟢
        if (type === '/cosmos.staking.v1beta1.MsgCreateValidator' || type === '/cosmos.staking.v1beta1.MsgEditValidator') {
          validatorsRows.push({
            operator_address: m.validator_address || m.operator_address,
            moniker: m.description?.moniker,
            website: m.description?.website,
            details: m.description?.details,
            commission_rate: parseDec(m.commission?.rate || m.commission_rate),
            max_commission_rate: parseDec(m.commission?.max_rate),
            max_change_rate: parseDec(m.commission?.max_change_rate),
            min_self_delegation: m.min_self_delegation,
            status: 'BOND_STATUS_BONDED', // Default, would need Query or Event for real-time
            updated_at_height: height,
            updated_at_time: time
          });
        }

        if (type === '/cosmos.staking.v1beta1.MsgDelegate' || type === '/cosmos.staking.v1beta1.MsgUndelegate') {
          const coin = m.amount;
          stakeDelegRows.push({
            height, tx_hash, msg_index: i,
            event_type: type.includes('Undelegate') ? 'undelegate' : 'delegate',
            delegator_address: m.delegator_address,
            validator_src: null,
            validator_dst: m.validator_address,
            denom: coin?.denom, amount: coin?.amount,
            completion_time: null
          });
        }
        if (type === '/cosmos.staking.v1beta1.MsgBeginRedelegate') {
          const coin = m.amount;
          stakeDelegRows.push({
            height, tx_hash, msg_index: i,
            event_type: 'redelegate',
            delegator_address: m.delegator_address,
            validator_src: m.validator_src_address,
            validator_dst: m.validator_dst_address,
            denom: coin?.denom, amount: coin?.amount,
            completion_time: null
          });
        }

        // 🟢 DISTRIBUTION LOGIC 🟢
        if (type === '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward') {
          const event = msgLog?.events.find((e: any) => e.type === 'withdraw_rewards');
          const amountStr = event ? findAttr(attrsToPairs(event.attributes), 'amount') : null;
          const coins = parseCoins(amountStr);
          for (const coin of coins) {
            stakeDistrRows.push({
              height, tx_hash, msg_index: i,
              event_type: 'withdraw_reward',
              delegator_address: m.delegator_address,
              validator_address: m.validator_address,
              denom: coin.denom, amount: coin.amount,
              withdraw_address: null
            });
          }
        }
        if (type === '/cosmos.distribution.v1beta1.MsgSetWithdrawAddress') {
          stakeDistrRows.push({
            height, tx_hash, msg_index: i,
            event_type: 'set_withdraw_address',
            delegator_address: m.delegator_address,
            withdraw_address: m.withdraw_address,
            validator_address: null, denom: null, amount: null
          });
        }

        // 🟢 IBC MSG_TRANSFER INTENT (OUTGOING) 🟢
        if (type === '/ibc.applications.transfer.v1.MsgTransfer') {
          txIbcIntents.push({
            msg_index: i,
            port: m.source_port,
            channel: m.source_channel,
            sender: m.sender,
            receiver: m.receiver,
            denom: m.token?.denom,
            amount: m.token?.amount,
            memo: m.memo ?? null
          });
        }

        // 🟢 MSG_UPDATE_PARAMS (GENERIC) 🟢
        if (type.endsWith('.MsgUpdateParams') && code === 0) {
          const moduleMatch = type.match(/^\/([^.]+)\./);
          const module = moduleMatch ? moduleMatch[1] : 'unknown';
          const params = m.params || {};

          networkParamsRows.push({
            height,
            time,
            module,
            param_key: '_all', // Modern SDK updates usually replace the whole params object
            old_value: null,
            new_value: params
          });
        }
      }

      // --- PROCESS LOGS (Events) ---
      for (const log of logs) {
        const msg_index = Number(log?.msg_index ?? -1);
        const events = normArray<any>(log?.events);

        // 🔄 Pre-scan message events for IBC metadata
        let msgIbcMeta: any = {};
        for (const ev of events) {
          if (ev.type === 'fungible_token_packet') {
            const ap = attrsToPairs(ev.attributes);
            msgIbcMeta.sender = findAttr(ap, 'sender');
            msgIbcMeta.receiver = findAttr(ap, 'receiver');
            msgIbcMeta.amount = findAttr(ap, 'amount');
            msgIbcMeta.denom = findAttr(ap, 'denom');
          }
        }

        for (let ei = 0; ei < events.length; ei++) {
          const ev = events[ei];
          const event_type = String(ev?.type ?? 'unknown');
          const attrsPairs = attrsToPairs(ev?.attributes);

          evRows.push({
            tx_hash, msg_index, event_index: ei, event_type,
            attributes: attrsPairs, height
          });

          if (event_type === 'wasm') {
            const contract = findAttr(attrsPairs, 'contract_address') || findAttr(attrsPairs, '_contract_address');
            if (contract) {
              wasmEventsRows.push({
                contract, height, tx_hash, msg_index, event_index: ei, event_type, attributes: attrsPairs
              });
              for (let ai = 0; ai < attrsPairs.length; ai++) {
                const attr = attrsPairs[ai];
                if (!attr) continue;
                wasmEventAttrsRows.push({
                  contract,
                  height,
                  tx_hash,
                  msg_index,
                  event_index: ei,
                  attr_index: ai,
                  key: attr.key,
                  value: attr.value
                });
              }

              // ✅ ENHANCED CW20 DETECTION (Multiple patterns)
              const action = findAttr(attrsPairs, 'action');
              const method = findAttr(attrsPairs, 'method');

              // Pattern 1: action=transfer/send (standard CW20)
              // Pattern 2: method=transfer/send (some CW20 variants)
              // Pattern 3: Heuristic - from/to/amount without action (non-standard)
              const hasFromTo = findAttr(attrsPairs, 'from') && findAttr(attrsPairs, 'to');
              const isCw20Transfer =
                action === 'transfer' || action === 'send' ||
                action === 'transfer_from' || action === 'send_from' ||
                method === 'transfer' || method === 'send' ||
                (!action && !method && hasFromTo && findAttr(attrsPairs, 'amount'));

              if (isCw20Transfer) {
                const fromAddr =
                  findAttr(attrsPairs, 'sender') ||
                  findAttr(attrsPairs, 'from') ||
                  findAttr(attrsPairs, 'from_address') ||
                  findAttr(attrsPairs, 'owner'); // For transfer_from
                const toAddr =
                  findAttr(attrsPairs, 'recipient') ||
                  findAttr(attrsPairs, 'to') ||
                  findAttr(attrsPairs, 'to_address');
                const amtRaw = findAttr(attrsPairs, 'amount');
                const amtCoin = amtRaw ? parseCoin(amtRaw) : null;
                const amount = (amtRaw && /^\d+$/.test(amtRaw)) ? amtRaw : amtCoin?.amount ?? null;
                if (fromAddr && toAddr && amount) {
                  cw20TransfersRows.push({
                    contract,
                    from_addr: fromAddr,
                    to_addr: toAddr,
                    amount,
                    height,
                    tx_hash
                  });
                }
              }

              // ✅ WASM DEX SWAP EXTRACTION (Astroport/TerraSwap style)
              if (action === 'swap') {
                const sender = findAttr(attrsPairs, 'sender');
                const receiver = findAttr(attrsPairs, 'receiver') || sender;
                const offerAsset = findAttr(attrsPairs, 'offer_asset');
                const askAsset = findAttr(attrsPairs, 'ask_asset');
                const offerAmount = findAttr(attrsPairs, 'offer_amount');
                const returnAmount = findAttr(attrsPairs, 'return_amount');
                const spreadAmount = findAttr(attrsPairs, 'spread_amount');
                const commissionAmount = findAttr(attrsPairs, 'commission_amount');
                const makerFeeAmount = findAttr(attrsPairs, 'maker_fee_amount');
                const feeShareAmount = findAttr(attrsPairs, 'fee_share_amount');
                const reservesRaw = findAttr(attrsPairs, 'reserves');
                let reserves = null;
                if (reservesRaw) {
                  // Handle both "amountDenom" and "denom:amount" formats
                  const obj: Record<string, string> = {};
                  const parts = reservesRaw.split(',');
                  for (const p of parts) {
                    const trimmed = p.trim();
                    if (!trimmed) continue;
                    // Try denom:amount
                    const colonIdx = trimmed.lastIndexOf(':');
                    if (colonIdx > 0) {
                      const d = trimmed.slice(0, colonIdx);
                      const a = trimmed.slice(colonIdx + 1).replace(/\D/g, ''); // sanitize amount
                      if (d && a) obj[d] = a;
                    } else {
                      // fallback to standard parseCoin
                      const c = parseCoin(trimmed);
                      if (c) obj[c.denom] = c.amount;
                    }
                  }
                  reserves = Object.keys(obj).length > 0 ? JSON.stringify(obj) : JSON.stringify({ raw: reservesRaw });
                }

                if (sender && offerAmount) {
                  // ✅ Compute analytics columns with NaN validation
                  const offerNum = parseFloat(offerAmount || '0');
                  const returnNum = parseFloat(returnAmount || '0');
                  const spreadNum = parseFloat(spreadAmount || '0');
                  const commissionNum = parseFloat(commissionAmount || '0');
                  const makerFeeNum = parseFloat(makerFeeAmount || '0');
                  const feeShareNum = parseFloat(feeShareAmount || '0');

                  // Generate sorted pair_id for consistent pair identification
                  const pairId = offerAsset && askAsset
                    ? [offerAsset, askAsset].sort().join('-')
                    : null;

                  // Effective price: return_amount / offer_amount (with NaN check)
                  const effectivePrice = offerNum > 0 && !isNaN(returnNum)
                    ? returnNum / offerNum
                    : null;

                  // Price impact (slippage %): (spread_amount / offer_amount) * 100 (with NaN check)
                  const priceImpact = offerNum > 0 && !isNaN(spreadNum)
                    ? (spreadNum / offerNum) * 100
                    : null;

                  // Total fee: sum of all fees (with NaN check)
                  const feeSum = (isNaN(commissionNum) ? 0 : commissionNum) +
                    (isNaN(makerFeeNum) ? 0 : makerFeeNum) +
                    (isNaN(feeShareNum) ? 0 : feeShareNum);
                  const totalFee = feeSum > 0 ? feeSum.toString() : '0';

                  wasmSwapsRows.push({
                    tx_hash,
                    msg_index,
                    event_index: ei,
                    contract,
                    sender,
                    receiver,
                    offer_asset: offerAsset,
                    ask_asset: askAsset,
                    offer_amount: toBigIntStr(offerAmount),
                    return_amount: toBigIntStr(returnAmount),
                    spread_amount: toBigIntStr(spreadAmount),
                    commission_amount: toBigIntStr(commissionAmount),
                    maker_fee_amount: toBigIntStr(makerFeeAmount),
                    fee_share_amount: toBigIntStr(feeShareAmount),
                    reserves,
                    // Analytics columns
                    pair_id: pairId,
                    effective_price: effectivePrice,
                    price_impact: priceImpact,
                    total_fee: totalFee,
                    block_height: height,
                    timestamp: time
                  });


                  // ✅ Track factory tokens discovered in swaps
                  for (const denom of [offerAsset, askAsset]) {
                    if (denom && denom.startsWith('coin.zig')) {
                      // Parse factory token format: coin.zigCREATOR.SYMBOL
                      const parts = denom.split('.');
                      if (parts.length >= 3) {
                        const creator = parts[1]; // e.g., "zig109f7g2rzl2aqee7z6gffn8kfe9cpqx0mjkk7ethmx8m2hq4xpe9snmaam2"
                        const symbol = parts.slice(2).join('.'); // e.g., "stzig"
                        factoryTokensRows.push({
                          denom,
                          base_denom: symbol,
                          creator,
                          symbol,
                          first_seen_height: height,
                          first_seen_tx: tx_hash
                        });
                      }
                    }
                  }
                }
              }

            }
          }

          if (event_type === 'instantiate' || event_type === '_instantiate') {
            const addr = findAttr(attrsPairs, '_contract_address') || findAttr(attrsPairs, 'contract_address');
            const codeId = findAttr(attrsPairs, 'code_id');
            if (addr && codeId) {
              wasmContractsRows.push({
                address: addr,
                code_id: toBigIntStr(codeId),
                creator: findAttr(attrsPairs, 'creator'),
                admin: findAttr(attrsPairs, 'admin'),
                label: findAttr(attrsPairs, 'label'),
                created_height: height,
                created_tx_hash: tx_hash
              });
            }
          }

          if (event_type === 'transfer') {
            const sender = findAttr(attrsPairs, 'sender');
            const recipient = findAttr(attrsPairs, 'recipient');
            const amountStr = findAttr(attrsPairs, 'amount');
            const coins = parseCoins(amountStr);
            for (const coin of coins) {
              transfersRows.push({
                tx_hash, msg_index, from_addr: sender, to_addr: recipient,
                denom: coin.denom, amount: coin.amount, height
              });
            }
          }

          // 🟢 IBC LOGIC 🟢
          if (['send_packet', 'recv_packet', 'acknowledge_packet', 'timeout_packet'].includes(event_type)) {
            const sequenceStr = findAttr(attrsPairs, 'packet_sequence');
            if (!sequenceStr || !/^\d+$/.test(sequenceStr)) {
              continue;
            }
            const sequence = toBigIntStr(sequenceStr);
            const srcPort = findAttr(attrsPairs, 'packet_src_port');
            const srcChan = findAttr(attrsPairs, 'packet_src_channel');
            const dstPort = findAttr(attrsPairs, 'packet_dst_port');
            const dstChan = findAttr(attrsPairs, 'packet_dst_channel');

            // ✅ FIX: Extract timeout fields
            const timeoutHeightStr = findAttr(attrsPairs, 'packet_timeout_height');
            const timeoutTsStr = findAttr(attrsPairs, 'packet_timeout_timestamp');

            if (sequence && srcChan) {
              // ✅ FIX: Try packet_data first, then packet_data_hex (hex-encoded)
              const dataJson = findAttr(attrsPairs, 'packet_data');
              const dataHex = findAttr(attrsPairs, 'packet_data_hex');
              let amount: string | null = null;
              let denom: string | null = null;
              let relayer: string | null = null;
              let memo: string | null = null;
              let sender: string | null = null;
              let receiver: string | null = null;

              try {
                // Try packet_data first (plain JSON), then packet_data_hex (hex-encoded)
                const d = dataJson ? tryParseJson(dataJson) : (dataHex ? decodeHexToJson(dataHex) : null);
                if (d) {
                  amount = d.amount ?? d.amt ?? msgIbcMeta.amount ?? null;
                  denom = d.denom ?? d.den ?? msgIbcMeta.denom ?? null;
                  sender = d.sender ?? d.snd ?? msgIbcMeta.sender ?? null;
                  receiver = d.receiver ?? d.rcv ?? msgIbcMeta.receiver ?? null;
                  memo = d.memo ?? null;
                }
              } catch { /* ignore parse error */ }

              // Fallback to fungible_token_packet event data if not extracted
              if (!sender) sender = msgIbcMeta.sender ?? null;
              if (!receiver) receiver = msgIbcMeta.receiver ?? null;
              if (!amount) amount = msgIbcMeta.amount ?? null;
              if (!denom) denom = msgIbcMeta.denom ?? null;

              // 🛡️ ENHANCEMENT: Link with MsgTransfer intents if fields are missing (Outgoing)
              if (event_type === 'send_packet') {
                const intent = txIbcIntents.find((it: any) => it.port === srcPort && it.channel === srcChan && !it.linked);
                if (intent) {
                  intent.linked = true; // Avoid double-link for multi-msg tx
                  amount = amount || intent.amount;
                  denom = denom || intent.denom;
                  sender = sender || intent.sender;
                  receiver = receiver || intent.receiver;
                  memo = memo || intent.memo;
                }
              }

              if (!relayer) {
                const ackRelayer = findAttr(attrsPairs, 'packet_ack_relayer') || findAttr(attrsPairs, 'relayer');
                if (event_type === 'send_packet') {
                  relayer = sender || findAttr(attrsPairs, 'packet_sender') || findAttr(attrsPairs, 'sender');
                } else {
                  // For recv, ack, timeout: the transaction signer is the relayer
                  relayer = firstSigner || ackRelayer;
                }
              }

              const statusMap: Record<string, string> = {
                send_packet: 'sent',
                recv_packet: 'received',
                acknowledge_packet: 'acknowledged',
                timeout_packet: 'timeout'
              };
              const status = statusMap[event_type] || 'failed';

              let ackSuccess: boolean | null = null;
              let ackError: string | null = null;
              if (event_type === 'acknowledge_packet') {
                const ackHex = findAttr(attrsPairs, 'packet_ack');
                if (ackHex) {
                  try {
                    const ackJson = tryParseJson(ackHex);
                    ackSuccess = !!ackJson.result;
                    ackError = ackJson.error || null;
                  } catch { /* ignore hex/binary parse errors for now */ }
                }
              }

              // Build packet row with event-specific columns
              const packetRow: any = {
                port_id_src: srcPort,
                channel_id_src: srcChan,
                sequence: Number(sequence), // Ensure numeric for BIGINT
                port_id_dst: dstPort,
                channel_id_dst: dstChan,
                timeout_height: timeoutHeightStr ?? null,
                timeout_ts: timeoutTsStr ?? null,
                status,
                denom,
                amount,
                sender,
                receiver,
                memo,
                // Event-specific fields
                tx_hash_send: null, height_send: null, time_send: null, relayer_send: null,
                tx_hash_recv: null, height_recv: null, time_recv: null, relayer_recv: null,
                tx_hash_ack: null, height_ack: null, time_ack: null, relayer_ack: null, ack_success: null, ack_error: null,
                tx_hash_timeout: null, height_timeout: null, time_timeout: null
              };

              // Populate event-specific fields
              if (event_type === 'send_packet') {
                packetRow.tx_hash_send = tx_hash;
                packetRow.height_send = height;
                packetRow.time_send = time;
                packetRow.relayer_send = relayer;
              } else if (event_type === 'recv_packet') {
                packetRow.tx_hash_recv = tx_hash;
                packetRow.height_recv = height;
                packetRow.time_recv = time;
                packetRow.relayer_recv = relayer;
              } else if (event_type === 'acknowledge_packet') {
                packetRow.tx_hash_ack = tx_hash;
                packetRow.height_ack = height;
                packetRow.time_ack = time;
                packetRow.relayer_ack = relayer;
                packetRow.ack_success = ackSuccess;
                packetRow.ack_error = ackError;
              } else if (event_type === 'timeout_packet') {
                packetRow.tx_hash_timeout = tx_hash;
                packetRow.height_timeout = height;
                packetRow.time_timeout = time;
              }

              ibcPacketsRows.push(packetRow);

              // ✅ FIX: Always record transfers for ALL lifecycle events (not just when denom/amount present)
              // This ensures ack/timeout statuses are properly recorded
              const transferRow: any = {
                port_id_src: srcPort,
                channel_id_src: srcChan,
                sequence: Number(sequence),
                port_id_dst: dstPort,
                channel_id_dst: dstChan,
                sender,
                receiver,
                denom,
                amount,
                memo,
                timeout_height: timeoutHeightStr ?? null,
                timeout_ts: timeoutTsStr ?? null,
                status,
                tx_hash_send: null, height_send: null, time_send: null, relayer_send: null,
                tx_hash_recv: null, height_recv: null, time_recv: null, relayer_recv: null,
                tx_hash_ack: null, height_ack: null, time_ack: null, relayer_ack: null, ack_success: null, ack_error: null,
                tx_hash_timeout: null, height_timeout: null, time_timeout: null
              };

              if (event_type === 'send_packet') {
                transferRow.tx_hash_send = tx_hash;
                transferRow.height_send = height;
                transferRow.time_send = time;
                transferRow.relayer_send = relayer;
              } else if (event_type === 'recv_packet') {
                transferRow.tx_hash_recv = tx_hash;
                transferRow.height_recv = height;
                transferRow.time_recv = time;
                transferRow.relayer_recv = relayer;
              } else if (event_type === 'acknowledge_packet') {
                transferRow.tx_hash_ack = tx_hash;
                transferRow.height_ack = height;
                transferRow.time_ack = time;
                transferRow.relayer_ack = relayer;
                transferRow.ack_success = ackSuccess;
                transferRow.ack_error = ackError;
              } else if (event_type === 'timeout_packet') {
                transferRow.tx_hash_timeout = tx_hash;
                transferRow.height_timeout = height;
                transferRow.time_timeout = time;
              }

              ibcTransfersRows.push(transferRow);
            }
          }

          if (event_type.startsWith('channel_open_') || event_type.startsWith('channel_close_')) {
            const portId = findAttr(attrsPairs, 'port_id') || findAttr(attrsPairs, 'packet_src_port');
            const channelId = findAttr(attrsPairs, 'channel_id') || findAttr(attrsPairs, 'packet_src_channel');
            if (portId && channelId) {
              const connectionId = findAttr(attrsPairs, 'connection_id');
              ibcChannelsRows.push({
                port_id: portId,
                channel_id: channelId,
                state: event_type,
                ordering: findAttr(attrsPairs, 'channel_ordering') || findAttr(attrsPairs, 'ordering'),
                connection_hops: connectionId ? [connectionId] : null,
                counterparty_port: findAttr(attrsPairs, 'counterparty_port_id') || findAttr(attrsPairs, 'counterparty_port'),
                counterparty_channel: findAttr(attrsPairs, 'counterparty_channel_id') || findAttr(attrsPairs, 'counterparty_channel'),
                version: findAttr(attrsPairs, 'version')
              });
            }
          }

          // 🟢 IBC CONNECTIONS 🟢
          if (event_type.startsWith('connection_open_')) {
            const connId = findAttr(attrsPairs, 'connection_id');
            const clientId = findAttr(attrsPairs, 'client_id');
            if (connId && clientId) {
              ibcConnectionsRows.push({
                connection_id: connId,
                client_id: clientId,
                counterparty_connection_id: findAttr(attrsPairs, 'counterparty_connection_id'),
                counterparty_client_id: findAttr(attrsPairs, 'counterparty_client_id'),
                state: event_type
              });
            }
          }


          // 🟢 IBC DISCOVERY (CLIENTS & DENOMS) 🟢
          // ✅ FIX: Handle both denom_trace and denomination events
          if (event_type === 'denom_trace' || event_type === 'denomination') {
            // Try denom_trace format first (hash + denom_trace path)
            let hash = findAttr(attrsPairs, 'hash') || findAttr(attrsPairs, 'denom_hash');
            let fullPath: string | null = null;
            let baseDenom: string | null = null;

            const denomTraceAttr = findAttr(attrsPairs, 'denom_trace');
            const denomAttr = findAttr(attrsPairs, 'denom');

            if (denomTraceAttr) {
              // Old format: denom_trace = "transfer/channel-0/uatom"
              fullPath = denomTraceAttr;
              baseDenom = denomTraceAttr.split('/').pop() || denomTraceAttr;
            } else if (denomAttr) {
              // New format: denom = {"base":"uaxl","trace":[{"port_id":"transfer","channel_id":"channel-0"}]}
              try {
                const d = tryParseJson(denomAttr);
                if (d && d.base) {
                  baseDenom = d.base;
                  // Reconstruct full path from trace array
                  if (Array.isArray(d.trace) && d.trace.length > 0) {
                    const tracePath = d.trace.map((t: any) => `${t.port_id}/${t.channel_id}`).join('/');
                    fullPath = `${tracePath}/${d.base}`;
                  } else {
                    fullPath = d.base;
                  }
                }
              } catch { /* ignore parse error */ }
            }

            if (hash && fullPath && baseDenom) {
              // Prepend ibc/ to hash if not already present
              const ibcHash = hash.startsWith('ibc/') ? hash : `ibc/${hash}`;
              ibcDenomsRows.push({ hash: ibcHash, full_path: fullPath, base_denom: baseDenom });
            }
          }

          if (event_type === 'create_client' || event_type === 'update_client') {
            const clientId = findAttr(attrsPairs, 'client_id');
            if (clientId) {
              ibcClientsRows.push({
                client_id: clientId,
                chain_id: findAttr(attrsPairs, 'chain_id'),
                client_type: findAttr(attrsPairs, 'client_type'),
                updated_at_height: height,
                updated_at_time: time
              });
            }
          }


          // 🟢 BANK BALANCE DELTAS 🟢
          extractBalanceDeltas(event_type, attrsPairs, tx_hash, msg_index, ei);


          // 🟢 NETWORK PARAMS 🟢
          if (event_type === 'param_change') {
            const module = findAttr(attrsPairs, 'module');
            const key = findAttr(attrsPairs, 'key');
            const value = findAttr(attrsPairs, 'value');
            if (module && key) {
              networkParamsRows.push({
                height,
                time,
                module,
                param_key: key,
                old_value: null, // Event typically only has new value
                new_value: tryParseJson(value)
              });
            }
          }

          // ✅ Phase 3: Specialized WASM Analytics Dispatcher
          if (event_type === 'wasm-temporal_numeric_value_update') {
            const contract = findAttr(attrsPairs, 'contract') || findAttr(attrsPairs, '_contract_address');
            const key = findAttr(attrsPairs, 'key');
            const value = findAttr(attrsPairs, 'value');
            if (contract && key && value) {
              wasmOracleUpdatesRows.push({
                height, tx_hash, msg_index, contract, key,
                value: parseFloat(value) || null
              });
            }
          }

          if (event_type === 'wasm-token_minted' || event_type === 'wasm-token_burned' || event_type === 'wasm-token_transferred') {
            const contract = findAttr(attrsPairs, 'contract') || findAttr(attrsPairs, '_contract_address');
            const amountAttr = findAttr(attrsPairs, 'amount');
            if (contract && amountAttr) {
              let action = 'transfer';
              if (event_type.includes('mint')) action = 'mint';
              else if (event_type.includes('burn')) action = 'burn';

              wasmTokenEventsRows.push({
                height, tx_hash, msg_index, contract,
                action,
                amount: toBigIntStr(amountAttr),
                recipient: findAttr(attrsPairs, 'recipient'),
                sender: findAttr(attrsPairs, 'sender')
              });
            }
          }

          for (let ai = 0; ai < attrsPairs.length; ai++) {
            const attr = attrsPairs[ai];
            if (!attr) continue;
            const { key, value } = attr;
            attrRows.push({ tx_hash, msg_index, event_index: ei, attr_index: ai, key, value, height });
          }
        }
      }

      // 🛡️ ENHANCEMENT: Final Flush for any unlinked IBC Intents in this TX
      // This ensures we capture MsgTransfer rows even if the send_packet event is missing or malformed.
      for (const intent of txIbcIntents) {
        if (!intent.linked) {
          ibcTransfersRows.push({
            height,
            tx_hash,
            msg_index: intent.msg_index,
            port_id_src: intent.port,
            channel_id_src: intent.channel,
            sequence: '0',
            sender: intent.sender,
            receiver: intent.receiver,
            denom: intent.denom,
            amount: intent.amount,
            memo: intent.memo,
            status: 'sent',
            tx_hash_send: tx_hash,
            height_send: height,
            time
          });
        }
      }
    }

    // ✅ FIX: Process Begin/End Block Events for Balances (e.g. Gov Refunds, Minting)
    const allBlockEvents = [
      ...(blockLine.block_results?.begin_block_events ?? []),
      ...(blockLine.block_results?.end_block_events ?? [])
    ];

    for (const ev of allBlockEvents) {
      const evType = ev.type;
      const attrsPairs = attrsToPairs(ev.attributes);
      extractBalanceDeltas(evType, attrsPairs);
    }


    return {
      blockRow, txRows, msgRows, evRows, attrRows,
      transfersRows, wasmExecRows, wasmEventsRows,
      govVotesRows, govDepositsRows, govProposalsRows, // 👈 Returning Gov Data
      stakeDelegRows, stakeDistrRows, // 👈 Returning Staking Data
      validatorsRows, validatorSetRows, missedBlocksRows, // 👈 Returning Validator Data
      ibcPacketsRows, // 👈 Returning IBC Data
      ibcChannelsRows,
      ibcTransfersRows,
      ibcClientsRows,
      ibcDenomsRows,
      ibcConnectionsRows,
      authzGrantsRows,
      feeGrantsRows,
      cw20TransfersRows,
      factoryDenomsRows, dexPoolsRows, dexSwapsRows, dexLiquidityRows, wrapperSettingsRows, wrapperEventsRows,
      balanceDeltasRows, wasmCodesRows, wasmContractsRows, wasmMigrationsRows, wasmAdminChangesRows, networkParamsRows,
      wasmEventAttrsRows,
      wasmSwapsRows, factoryTokensRows, // ✅ WASM DEX Swaps Analytics
      unknownMsgsRows, // ✅ Unknown Messages Quarantine
      factorySupplyEventsRows,
      wasmOracleUpdatesRows,
      wasmTokenEventsRows,
      wasmInstantiateConfigsRows,
      height
    };
  }

  private async persistBlockBuffered(blockLine: BlockLine): Promise<void> {
    const data = this.extractRows(blockLine);

    this.bufBlocks.push(data.blockRow);
    this.bufTxs.push(...data.txRows);
    this.bufMsgs.push(...data.msgRows);
    this.bufEvents.push(...data.evRows);
    this.bufAttrs.push(...data.attrRows);
    this.bufTransfers.push(...data.transfersRows);

    // Zigchain
    this.bufFactoryDenoms.push(...data.factoryDenomsRows);
    this.bufDexPools.push(...data.dexPoolsRows);
    this.bufDexSwaps.push(...data.dexSwapsRows);
    this.bufDexLiquidity.push(...data.dexLiquidityRows);
    this.bufWrapperSettings.push(...data.wrapperSettingsRows);
    this.bufWrapperEvents.push(...data.wrapperEventsRows);
    this.bufWasmOracleUpdates.push(...data.wasmOracleUpdatesRows);
    this.bufWasmTokenEvents.push(...data.wasmTokenEventsRows);

    // WASM
    this.bufWasmExec.push(...data.wasmExecRows);
    this.bufWasmEvents.push(...data.wasmEventsRows);
    this.bufWasmEventAttrs.push(...data.wasmEventAttrsRows);

    // ✅ Gov (Pushing to Buffers)
    this.bufGovVotes.push(...data.govVotesRows);
    this.bufGovDeposits.push(...data.govDepositsRows);
    this.bufGovProposals.push(...data.govProposalsRows);

    // ✅ Staking (Pushing to Buffers)
    this.bufStakeDeleg.push(...data.stakeDelegRows);
    this.bufStakeDistr.push(...data.stakeDistrRows);

    // ✅ ADDED: New Buffers Persistence
    this.bufBalanceDeltas.push(...data.balanceDeltasRows);
    this.bufWasmCodes.push(...data.wasmCodesRows);
    this.bufWasmContracts.push(...data.wasmContractsRows);
    this.bufWasmMigrations.push(...data.wasmMigrationsRows);
    this.bufWasmAdminChanges.push(...data.wasmAdminChangesRows);
    this.bufWasmInstantiateConfigs.push(...data.wasmInstantiateConfigsRows);
    this.bufNetworkParams.push(...data.networkParamsRows);

    // ✅ Validator (Pushing to Buffer)
    this.bufValidators.push(...data.validatorsRows);
    this.bufValidatorSet.push(...data.validatorSetRows);
    this.bufMissedBlocks.push(...data.missedBlocksRows);

    // ✅ IBC (Pushing to Buffer)
    this.bufIbcPackets.push(...data.ibcPacketsRows);
    this.bufIbcChannels.push(...data.ibcChannelsRows);
    this.bufIbcTransfers.push(...data.ibcTransfersRows);
    this.bufIbcClients.push(...data.ibcClientsRows);
    this.bufIbcDenoms.push(...data.ibcDenomsRows);
    this.bufIbcConnections.push(...data.ibcConnectionsRows);

    // ✅ Authz / Feegrant (Pushing to Buffer)
    this.bufAuthzGrants.push(...data.authzGrantsRows);
    this.bufFeeGrants.push(...data.feeGrantsRows);

    // ✅ Tokens (CW20) (Pushing to Buffer)
    this.bufCw20Transfers.push(...data.cw20TransfersRows);

    // ✅ WASM DEX Swaps Analytics (Pushing to Buffer)
    this.bufWasmSwaps.push(...data.wasmSwapsRows);
    this.bufFactoryTokens.push(...data.factoryTokensRows);

    // ✅ Unknown Messages Quarantine (Pushing to Buffer)
    this.bufUnknownMsgs.push(...data.unknownMsgsRows);
    this.bufFactorySupplyEvents.push(...data.factorySupplyEventsRows);

    const zCount = this.bufFactoryDenoms.length + this.bufDexPools.length + this.bufDexSwaps.length + this.bufDexLiquidity.length;
    const gCount = this.bufGovVotes.length + this.bufGovDeposits.length;

    const needFlush =
      this.bufBlocks.length >= this.batchSizes.blocks ||
      this.bufTxs.length >= this.batchSizes.txs ||
      zCount >= this.batchSizes.zigchain ||
      gCount >= this.batchSizes.govVotes;

    if (needFlush) {
      log.debug(`flush trigger: blocks=${this.bufBlocks.length}`);
      await this.flushAll();
    }
  }

  private async flushAll(): Promise<void> {
    if (this.bufBlocks.length === 0 && this.bufTxs.length === 0) return;

    // ✅ SNAPSHOT & SWAP BUFFERS
    // We move all current data to a local "snapshot" and clear the main buffers.
    // This allows incoming writes to continue safely while we are awaiting the DB.
    const snapshot = {
      blocks: this.bufBlocks,
      txs: this.bufTxs,
      msgs: this.bufMsgs,
      events: this.bufEvents,
      attrs: this.bufAttrs,
      transfers: this.bufTransfers,
      factoryDenoms: this.bufFactoryDenoms,
      dexPools: this.bufDexPools,
      dexSwaps: this.bufDexSwaps,
      dexLiquidity: this.bufDexLiquidity,
      wrapperSettings: this.bufWrapperSettings,
      wrapperEvents: this.bufWrapperEvents,
      wasmOracleUpdates: this.bufWasmOracleUpdates,
      wasmTokenEvents: this.bufWasmTokenEvents,
      wasmExec: this.bufWasmExec,
      wasmEvents: this.bufWasmEvents,
      wasmEventAttrs: this.bufWasmEventAttrs,
      govVotes: this.bufGovVotes,
      govDeposits: this.bufGovDeposits,
      govProposals: this.bufGovProposals,
      stakeDeleg: this.bufStakeDeleg,
      stakeDistr: this.bufStakeDistr,
      balanceDeltas: this.bufBalanceDeltas,
      wasmCodes: this.bufWasmCodes,
      wasmContracts: this.bufWasmContracts,
      wasmMigrations: this.bufWasmMigrations,
      wasmAdminChanges: this.bufWasmAdminChanges,
      wasmInstantiateConfigs: this.bufWasmInstantiateConfigs,
      networkParams: this.bufNetworkParams,
      validators: this.bufValidators,
      validatorSet: this.bufValidatorSet,
      missedBlocks: this.bufMissedBlocks,
      ibcPackets: this.bufIbcPackets,
      ibcChannels: this.bufIbcChannels,
      ibcTransfers: this.bufIbcTransfers,
      ibcClients: this.bufIbcClients,
      ibcDenoms: this.bufIbcDenoms,
      ibcConnections: this.bufIbcConnections,
      authzGrants: this.bufAuthzGrants,
      feeGrants: this.bufFeeGrants,
      cw20Transfers: this.bufCw20Transfers,
      wasmSwaps: this.bufWasmSwaps,
      factoryTokens: this.bufFactoryTokens,
      unknownMsgs: this.bufUnknownMsgs,
      factorySupplyEvents: this.bufFactorySupplyEvents,
    };

    // Reset all main buffers
    this.bufBlocks = []; this.bufTxs = []; this.bufMsgs = []; this.bufEvents = []; this.bufAttrs = []; this.bufTransfers = [];
    this.bufFactoryDenoms = []; this.bufDexPools = []; this.bufDexSwaps = []; this.bufDexLiquidity = [];
    this.bufWrapperSettings = []; this.bufWrapperEvents = []; this.bufWasmOracleUpdates = []; this.bufWasmTokenEvents = [];
    this.bufWasmExec = []; this.bufWasmEvents = []; this.bufWasmEventAttrs = [];
    this.bufGovVotes = []; this.bufGovDeposits = []; this.bufGovProposals = [];
    this.bufStakeDeleg = []; this.bufStakeDistr = [];
    this.bufBalanceDeltas = []; this.bufWasmCodes = []; this.bufWasmContracts = []; this.bufWasmMigrations = [];
    this.bufWasmAdminChanges = []; this.bufWasmInstantiateConfigs = []; this.bufNetworkParams = [];
    this.bufValidators = []; this.bufValidatorSet = []; this.bufMissedBlocks = [];
    this.bufIbcPackets = []; this.bufIbcChannels = []; this.bufIbcTransfers = [];
    this.bufIbcClients = []; this.bufIbcDenoms = []; this.bufIbcConnections = [];
    this.bufAuthzGrants = []; this.bufFeeGrants = []; this.bufCw20Transfers = [];
    this.bufWasmSwaps = []; this.bufFactoryTokens = []; this.bufUnknownMsgs = [];
    this.bufFactorySupplyEvents = [];

    const pool = getPgPool();
    const client = await pool.connect();

    try {
      const heights = snapshot.blocks.map(r => r.height);
      const minH = Math.min(...heights);
      const maxH = Math.max(...heights);

      await ensureCorePartitions(client, minH, maxH);
      await client.query('BEGIN');

      // 1. Standard
      await flushBlocks(client, snapshot.blocks);
      await flushTxs(client, snapshot.txs);
      await flushMsgs(client, snapshot.msgs);
      await flushEvents(client, snapshot.events);
      await flushAttrs(client, snapshot.attrs);
      await flushTransfers(client, snapshot.transfers);

      // 2. Modules (WASM & Gov)
      await flushWasmExec(client, snapshot.wasmExec);
      await flushWasmEvents(client, snapshot.wasmEvents);
      await flushWasmEventAttrs(client, snapshot.wasmEventAttrs);

      await flushGovVotes(client, snapshot.govVotes);
      await flushGovDeposits(client, snapshot.govDeposits);
      await upsertGovProposals(client, snapshot.govProposals);

      await flushAuthzGrants(client, snapshot.authzGrants);
      await flushFeeGrants(client, snapshot.feeGrants);
      await flushCw20Transfers(client, snapshot.cw20Transfers);

      await flushStakeDeleg(client, snapshot.stakeDeleg);
      await flushStakeDistr(client, snapshot.stakeDistr);

      await upsertValidators(client, snapshot.validators);
      await execBatchedInsert(client, 'core.validator_set', ['height', 'operator_address', 'voting_power', 'proposer_priority'], snapshot.validatorSet, 'ON CONFLICT (height, operator_address) DO NOTHING');
      await execBatchedInsert(client, 'core.validator_missed_blocks', ['operator_address', 'height'], snapshot.missedBlocks, 'ON CONFLICT (operator_address, height) DO NOTHING');

      // IBC Data
      if (snapshot.ibcPackets.length > 0 || snapshot.ibcTransfers.length > 0) {
        const seqs = [...snapshot.ibcPackets, ...snapshot.ibcTransfers]
          .map((r: any) => Number(r.sequence))
          .filter((n: number) => Number.isFinite(n));
        if (seqs.length > 0) {
          const minSeq = Math.min(...seqs);
          const maxSeq = Math.max(...seqs);
          await ensureIbcPartitions(client, minSeq, maxSeq);
        }
      }
      await flushIbcPackets(client, snapshot.ibcPackets);
      await flushIbcChannels(client, snapshot.ibcChannels);
      await flushIbcTransfers(client, snapshot.ibcTransfers);
      await flushIbcClients(client, snapshot.ibcClients);
      await flushIbcDenoms(client, snapshot.ibcDenoms);
      await flushIbcConnections(client, snapshot.ibcConnections);

      if (snapshot.balanceDeltas.length > 0) {
        await flushBalanceDeltas(client, snapshot.balanceDeltas);
      }

      if (snapshot.wasmCodes.length > 0 || snapshot.wasmContracts.length > 0 || snapshot.wasmMigrations.length > 0 || snapshot.wasmInstantiateConfigs.length > 0) {
        await flushWasmRegistry(client, {
          codes: snapshot.wasmCodes,
          contracts: snapshot.wasmContracts,
          migrations: snapshot.wasmMigrations,
          configs: snapshot.wasmInstantiateConfigs
        });
      }
      if (snapshot.wasmAdminChanges.length > 0) {
        await flushWasmAdminChanges(client, snapshot.wasmAdminChanges);
      }
      if (snapshot.networkParams.length > 0) {
        await flushNetworkParams(client, snapshot.networkParams);
      }

      // WASM Analytics
      if (snapshot.wasmSwaps.length > 0) {
        await flushWasmSwaps(client, snapshot.wasmSwaps);
      }
      if (snapshot.factoryTokens.length > 0) {
        await flushFactoryTokens(client, snapshot.factoryTokens);
      }
      if (snapshot.wasmOracleUpdates.length > 0 || snapshot.wasmTokenEvents.length > 0) {
        await flushWasmAnalytics(client, {
          oracleUpdates: snapshot.wasmOracleUpdates,
          tokenEvents: snapshot.wasmTokenEvents
        });
      }

      if (snapshot.unknownMsgs.length > 0) {
        await flushUnknownMessages(client, snapshot.unknownMsgs);
      }

      if (snapshot.factorySupplyEvents.length > 0) {
        await flushFactorySupplyEvents(client, snapshot.factorySupplyEvents);
      }

      // 3. Zigchain
      await flushZigchainData(client, {
        factoryDenoms: snapshot.factoryDenoms,
        dexPools: snapshot.dexPools,
        dexSwaps: snapshot.dexSwaps,
        dexLiquidity: snapshot.dexLiquidity,
        wrapperSettings: snapshot.wrapperSettings,
        wrapperEvents: snapshot.wrapperEvents
      });

      await upsertProgress(client, this.cfg.pg?.progressId ?? 'default', maxH);
      await client.query('COMMIT');
    } catch (e) {
      log.error(`[flush-error] rollback initiated: ${String(e)}`);
      await client.query('ROLLBACK');

      // ❌ RESTORE BUFFERS ON FAILURE
      // We prepend the snapshot data to the current buffers so they are included in the next retry.
      this.bufBlocks = [...snapshot.blocks, ...this.bufBlocks];
      this.bufTxs = [...snapshot.txs, ...this.bufTxs];
      this.bufMsgs = [...snapshot.msgs, ...this.bufMsgs];
      this.bufEvents = [...snapshot.events, ...this.bufEvents];
      this.bufAttrs = [...snapshot.attrs, ...this.bufAttrs];
      this.bufTransfers = [...snapshot.transfers, ...this.bufTransfers];
      this.bufFactoryDenoms = [...snapshot.factoryDenoms, ...this.bufFactoryDenoms];
      this.bufDexPools = [...snapshot.dexPools, ...this.bufDexPools];
      this.bufDexSwaps = [...snapshot.dexSwaps, ...this.bufDexSwaps];
      this.bufDexLiquidity = [...snapshot.dexLiquidity, ...this.bufDexLiquidity];
      this.bufWrapperSettings = [...snapshot.wrapperSettings, ...this.bufWrapperSettings];
      this.bufWrapperEvents = [...snapshot.wrapperEvents, ...this.bufWrapperEvents];
      this.bufWasmOracleUpdates = [...snapshot.wasmOracleUpdates, ...this.bufWasmOracleUpdates];
      this.bufWasmTokenEvents = [...snapshot.wasmTokenEvents, ...this.bufWasmTokenEvents];
      this.bufWasmExec = [...snapshot.wasmExec, ...this.bufWasmExec];
      this.bufWasmEvents = [...snapshot.wasmEvents, ...this.bufWasmEvents];
      this.bufWasmEventAttrs = [...snapshot.wasmEventAttrs, ...this.bufWasmEventAttrs];
      this.bufGovVotes = [...snapshot.govVotes, ...this.bufGovVotes];
      this.bufGovDeposits = [...snapshot.govDeposits, ...this.bufGovDeposits];
      this.bufGovProposals = [...snapshot.govProposals, ...this.bufGovProposals];
      this.bufStakeDeleg = [...snapshot.stakeDeleg, ...this.bufStakeDeleg];
      this.bufStakeDistr = [...snapshot.stakeDistr, ...this.bufStakeDistr];
      this.bufBalanceDeltas = [...snapshot.balanceDeltas, ...this.bufBalanceDeltas];
      this.bufWasmCodes = [...snapshot.wasmCodes, ...this.bufWasmCodes];
      this.bufWasmContracts = [...snapshot.wasmContracts, ...this.bufWasmContracts];
      this.bufWasmMigrations = [...snapshot.wasmMigrations, ...this.bufWasmMigrations];
      this.bufWasmAdminChanges = [...snapshot.wasmAdminChanges, ...this.bufWasmAdminChanges];
      this.bufWasmInstantiateConfigs = [...snapshot.wasmInstantiateConfigs, ...this.bufWasmInstantiateConfigs];
      this.bufNetworkParams = [...snapshot.networkParams, ...this.bufNetworkParams];
      this.bufValidators = [...snapshot.validators, ...this.bufValidators];
      this.bufValidatorSet = [...snapshot.validatorSet, ...this.bufValidatorSet];
      this.bufMissedBlocks = [...snapshot.missedBlocks, ...this.bufMissedBlocks];
      this.bufIbcPackets = [...snapshot.ibcPackets, ...this.bufIbcPackets];
      this.bufIbcChannels = [...snapshot.ibcChannels, ...this.bufIbcChannels];
      this.bufIbcTransfers = [...snapshot.ibcTransfers, ...this.bufIbcTransfers];
      this.bufIbcClients = [...snapshot.ibcClients, ...this.bufIbcClients];
      this.bufIbcDenoms = [...snapshot.ibcDenoms, ...this.bufIbcDenoms];
      this.bufIbcConnections = [...snapshot.ibcConnections, ...this.bufIbcConnections];
      this.bufAuthzGrants = [...snapshot.authzGrants, ...this.bufAuthzGrants];
      this.bufFeeGrants = [...snapshot.feeGrants, ...this.bufFeeGrants];
      this.bufCw20Transfers = [...snapshot.cw20Transfers, ...this.bufCw20Transfers];
      this.bufWasmSwaps = [...snapshot.wasmSwaps, ...this.bufWasmSwaps];
      this.bufFactoryTokens = [...snapshot.factoryTokens, ...this.bufFactoryTokens];
      this.bufUnknownMsgs = [...snapshot.unknownMsgs, ...this.bufUnknownMsgs];
      this.bufFactorySupplyEvents = [...snapshot.factorySupplyEvents, ...this.bufFactorySupplyEvents];

      throw e;
    } finally {
      client.release();
    }
  }

  // Atomic mode stub
  private async persistBlockAtomic(blockLine: BlockLine): Promise<void> { return; }
}
