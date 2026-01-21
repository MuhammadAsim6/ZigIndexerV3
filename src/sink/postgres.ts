/**
 * PostgreSQL sink implementation.
 * Updated for Zigchain Custom Modules (DEX, Liquidity, WASM) AND Governance.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Sink, SinkConfig } from './types.js';
import { createPgPool, getPgPool, closePgPool } from '../db/pg.js';
import { ensureCorePartitions, ensureIbcPartitions } from '../db/partitions.js';
import { upsertProgress } from '../db/progress.js';
import { recordMissingBlock, resolveMissingBlock } from '../db/missing_blocks.js';
import { getLogger } from '../utils/logger.js';
import {
  pickMessages,
  pickLogs,
  attrsToPairs,
  toNum,
  buildFeeFromDecodedFee,
  collectSignersFromMessages,
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
    progressId?: string;
  };
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
  private bufNetworkParams: any[] = [];

  // Zigchain Buffers
  private bufFactoryDenoms: any[] = [];
  private bufDexPools: any[] = [];
  private bufDexSwaps: any[] = [];
  private bufDexLiquidity: any[] = [];
  private bufWrapperSettings: any[] = [];
  private bufCw20Transfers: any[] = [];

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
      size_bytes: b?.block?.size ?? null,
      last_commit_hash: b?.block?.last_commit?.block_id?.hash ?? null,
      data_hash: b?.block?.data?.hash ?? null,
      evidence_count: Array.isArray(b?.block?.evidence?.evidence) ? b.block.evidence.evidence.length : 0,
      app_hash: b?.block?.header?.app_hash ?? null,
    };

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
    const networkParamsRows: any[] = [];

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

        msgRows.push({
          tx_hash, msg_index: i, height, type_url: type, value: m,
          signer: m?.signer ?? m?.from_address ?? m?.delegator_address ?? null,
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
              voter: m.voter,
              option: opt.option?.toString() ?? 'VOTE_OPTION_UNSPECIFIED',
              weight: opt.weight ?? '1.0', // Weight is a decimal string like "0.5"
              height,
              tx_hash
            });
          }
        }

        // 2. DEPOSIT
        if (type === '/cosmos.gov.v1beta1.MsgDeposit' || type === '/cosmos.gov.v1.MsgDeposit') {
          const amounts = Array.isArray(m.amount) ? m.amount : [m.amount];
          for (const coin of amounts) {
            if (!coin) continue;
            govDepositsRows.push({
              proposal_id: m.proposal_id,
              depositor: m.depositor,
              amount: coin.amount,
              denom: coin.denom,
              height,
              tx_hash
            });
          }
        }

        // 3. PROPOSAL (Only Success - ID is generated on-chain)
        if ((type === '/cosmos.gov.v1beta1.MsgSubmitProposal' || type === '/cosmos.gov.v1.MsgSubmitProposal') && code === 0) {
          const event = msgLog?.events.find((e: any) => e.type === 'submit_proposal');
          const pid = findAttr(attrsToPairs(event?.attributes), 'proposal_id');

          if (pid) {
            // ✅ FIX: Extract submitter and proposal_type
            const submitter = m.proposer || m.signer || firstSigner;
            // v1beta1 uses content.@type, v1 uses messages array
            const proposalType = m.content?.['@type'] ??
              (Array.isArray(m.messages) && m.messages[0]?.['@type']) ??
              findAttr(attrsToPairs(event?.attributes), 'proposal_type') ??
              null;

            govProposalsRows.push({
              proposal_id: pid,
              submitter,
              title: m.content?.title ?? m.title ?? '',
              summary: m.content?.description ?? m.summary ?? '',
              proposal_type: proposalType,
              submit_time: time,
              status: 'deposit_period',
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
        if (type === '/cosmwasm.wasm.v1.MsgStoreCode') {
          const event = msgLog?.events.find((e: any) => e.type === 'store_code');
          const attrs = attrsToPairs(event?.attributes);
          const codeId = findAttr(attrs, 'code_id');
          const checksum = findAttr(attrs, 'code_checksum') || findAttr(attrs, 'checksum') || m.checksum;

          if (codeId) {
            wasmCodesRows.push({
              code_id: codeId,
              checksum: checksum || '', // Ensure not null
              creator: m.sender,
              instantiate_permission: m.instantiate_permission,
              store_tx_hash: tx_hash,
              store_height: height
            });
          }
        }

        if (type === '/cosmwasm.wasm.v1.MsgInstantiateContract' ||
          type === '/cosmwasm.wasm.v1.MsgInstantiateContract2') {
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

        if (type === '/cosmwasm.wasm.v1.MsgMigrateContract') {
          wasmMigrationsRows.push({
            contract: m.contract,
            from_code_id: null, // Would need Query to find previous, but we track the 'to' here
            to_code_id: m.code_id,
            height,
            tx_hash
          });
        }

        // 🟢 ZIGCHAIN LOGIC 🟢
        if (type.endsWith('.MsgCreateDenom')) {
          const event = msgLog?.events.find((e: any) => e.type === 'create_denom');
          let finalDenom = event ? findAttr(attrsToPairs(event.attributes), 'denom') : `factory/${m.creator}/${m.sub_denom}`;

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
          dexSwapsRows.push({
            tx_hash,
            msg_index: i,
            pool_id: m.pool_id,
            sender_address: m.signer || m.creator || firstSigner,
            token_in_denom: m.incoming?.denom || m.incoming_max?.denom,
            token_in_amount: m.incoming?.amount || m.incoming_max?.amount,
            token_out_denom: m.outgoing?.denom || m.outgoing_min?.denom,
            token_out_amount: m.outgoing?.amount || m.outgoing_min?.amount,
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
        if (type === '/cosmwasm.wasm.v1.MsgExecuteContract') {
          wasmExecRows.push({
            tx_hash, msg_index: i, contract: m?.contract ?? m?.contract_address, caller: m?.sender,
            funds: m?.funds, msg: tryParseJson(m?.msg), success: code === 0, error: code === 0 ? null : (log_summary),
            gas_used, height
          });
        }

        // 🟢 WASM ADMIN CHANGES (Security Auditing) 🟢
        if (type === '/cosmwasm.wasm.v1.MsgUpdateAdmin' && code === 0) {
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

        if (type === '/cosmwasm.wasm.v1.MsgClearAdmin' && code === 0) {
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

              const action = findAttr(attrsPairs, 'action');
              if (action === 'transfer' || action === 'send') {
                const fromAddr =
                  findAttr(attrsPairs, 'sender') ||
                  findAttr(attrsPairs, 'from') ||
                  findAttr(attrsPairs, 'from_address');
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
          if (event_type === 'coin_received' || event_type === 'coin_spent') {
            const acc = findAttr(attrsPairs, event_type === 'coin_received' ? 'receiver' : 'spender');
            const amountStr = findAttr(attrsPairs, 'amount');
            const coins = parseCoins(amountStr);
            for (const coin of coins) {
              balanceDeltasRows.push({
                height,
                account: acc,
                denom: coin.denom,
                delta: event_type === 'coin_received' ? coin.amount : `-${coin.amount}`
              });
            }
          }

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

          for (const { key, value } of attrsPairs) {
            attrRows.push({ tx_hash, msg_index, event_index: ei, key, value, height });
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

    return {
      blockRow, txRows, msgRows, evRows, attrRows,
      transfersRows, wasmExecRows, wasmEventsRows,
      govVotesRows, govDepositsRows, govProposalsRows, // 👈 Returning Gov Data
      stakeDelegRows, stakeDistrRows, // 👈 Returning Staking Data
      validatorsRows, // 👈 Returning Validator Data
      ibcPacketsRows, // 👈 Returning IBC Data
      ibcChannelsRows,
      ibcTransfersRows,
      ibcClientsRows,
      ibcDenomsRows,
      ibcConnectionsRows,
      authzGrantsRows,
      feeGrantsRows,
      cw20TransfersRows,
      factoryDenomsRows, dexPoolsRows, dexSwapsRows, dexLiquidityRows, wrapperSettingsRows,
      balanceDeltasRows, wasmCodesRows, wasmContractsRows, wasmMigrationsRows, wasmAdminChangesRows, networkParamsRows,
      wasmEventAttrsRows,
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
    this.bufNetworkParams.push(...data.networkParamsRows);

    // ✅ Validator (Pushing to Buffer)
    this.bufValidators.push(...data.validatorsRows);

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

    const pool = getPgPool();
    const client = await pool.connect();
    try {
      const heights = this.bufBlocks.map(r => r.height);
      const minH = Math.min(...heights);
      const maxH = Math.max(...heights);

      await ensureCorePartitions(client, minH, maxH);
      await client.query('BEGIN');

      // 1. Standard
      await flushBlocks(client, this.bufBlocks); this.bufBlocks = [];
      await flushTxs(client, this.bufTxs); this.bufTxs = [];
      await flushMsgs(client, this.bufMsgs); this.bufMsgs = [];
      await flushEvents(client, this.bufEvents); this.bufEvents = [];
      await flushAttrs(client, this.bufAttrs); this.bufAttrs = [];
      await flushTransfers(client, this.bufTransfers); this.bufTransfers = [];

      // 2. Modules (WASM & Gov)
      await flushWasmExec(client, this.bufWasmExec); this.bufWasmExec = [];
      await flushWasmEvents(client, this.bufWasmEvents); this.bufWasmEvents = [];
      await flushWasmEventAttrs(client, this.bufWasmEventAttrs); this.bufWasmEventAttrs = [];

      // ✅ Flushing Gov Data
      await flushGovVotes(client, this.bufGovVotes); this.bufGovVotes = [];
      await flushGovDeposits(client, this.bufGovDeposits); this.bufGovDeposits = [];
      await upsertGovProposals(client, this.bufGovProposals); this.bufGovProposals = [];

      // ✅ Flushing Authz/Feegrant Data
      await flushAuthzGrants(client, this.bufAuthzGrants); this.bufAuthzGrants = [];
      await flushFeeGrants(client, this.bufFeeGrants); this.bufFeeGrants = [];

      // ✅ Flushing CW20 Data
      await flushCw20Transfers(client, this.bufCw20Transfers); this.bufCw20Transfers = [];

      // ✅ Flushing Staking Data
      await flushStakeDeleg(client, this.bufStakeDeleg); this.bufStakeDeleg = [];
      await flushStakeDistr(client, this.bufStakeDistr); this.bufStakeDistr = [];

      // ✅ Flushing Validator Data
      await upsertValidators(client, this.bufValidators); this.bufValidators = [];

      // ✅ Flushing IBC Data
      if (this.bufIbcPackets.length > 0) {
        const seqs = [...this.bufIbcPackets, ...this.bufIbcTransfers]
          .map((r: any) => Number(r.sequence))
          .filter((n: number) => Number.isFinite(n));
        if (seqs.length > 0) {
          const minSeq = Math.min(...seqs);
          const maxSeq = Math.max(...seqs);
          await ensureIbcPartitions(client, minSeq, maxSeq);
        }
      }
      await flushIbcPackets(client, this.bufIbcPackets); this.bufIbcPackets = [];
      await flushIbcChannels(client, this.bufIbcChannels); this.bufIbcChannels = [];
      await flushIbcTransfers(client, this.bufIbcTransfers); this.bufIbcTransfers = [];
      await flushIbcClients(client, this.bufIbcClients); this.bufIbcClients = [];
      await flushIbcDenoms(client, this.bufIbcDenoms); this.bufIbcDenoms = [];
      await flushIbcConnections(client, this.bufIbcConnections); this.bufIbcConnections = [];

      // ✅ ADDED: New Flushers
      if (this.bufBalanceDeltas.length > 0) {
        await flushBalanceDeltas(client, this.bufBalanceDeltas); this.bufBalanceDeltas = [];
      }
      if (this.bufWasmCodes.length > 0 || this.bufWasmContracts.length > 0 || this.bufWasmMigrations.length > 0) {
        await flushWasmRegistry(client, {
          codes: this.bufWasmCodes,
          contracts: this.bufWasmContracts,
          migrations: this.bufWasmMigrations
        });
        this.bufWasmCodes = []; this.bufWasmContracts = []; this.bufWasmMigrations = [];
      }
      if (this.bufWasmAdminChanges.length > 0) {
        await flushWasmAdminChanges(client, this.bufWasmAdminChanges); this.bufWasmAdminChanges = [];
      }
      if (this.bufNetworkParams.length > 0) {
        await flushNetworkParams(client, this.bufNetworkParams); this.bufNetworkParams = [];
      }

      // 3. Zigchain
      await flushZigchainData(client, {
        factoryDenoms: this.bufFactoryDenoms,
        dexPools: this.bufDexPools,
        dexSwaps: this.bufDexSwaps,
        dexLiquidity: this.bufDexLiquidity,
        wrapperSettings: this.bufWrapperSettings
      });
      this.bufFactoryDenoms = [];
      this.bufDexPools = [];
      this.bufDexSwaps = [];
      this.bufDexLiquidity = [];
      this.bufWrapperSettings = [];

      await upsertProgress(client, this.cfg.pg?.progressId ?? 'default', maxH);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  // Atomic mode stub
  private async persistBlockAtomic(blockLine: BlockLine): Promise<void> { return; }
}
