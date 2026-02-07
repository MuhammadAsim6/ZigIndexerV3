import type { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

// 1. Factory Denoms
export async function insertFactoryDenoms(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'creator_address', 'sub_denom', 'minting_cap',
    'uri', 'uri_hash', 'description', 'creation_tx_hash', 'block_height'
  ];
  await execBatchedInsert(
    client,
    'zigchain.factory_denoms',
    cols,
    rows,
    'ON CONFLICT (denom) DO UPDATE SET ' +
    'creator_address = EXCLUDED.creator_address, ' +
    'sub_denom = EXCLUDED.sub_denom, ' +
    'minting_cap = EXCLUDED.minting_cap, ' +
    'uri = EXCLUDED.uri, ' +
    'uri_hash = EXCLUDED.uri_hash, ' +
    'description = EXCLUDED.description, ' +
    'creation_tx_hash = EXCLUDED.creation_tx_hash, ' +
    'block_height = EXCLUDED.block_height'
  );
}

// 2. DEX Pools
export async function insertDexPools(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'pool_id', 'creator_address', 'pair_id', 'base_denom', 'quote_denom',
    'lp_token_denom', 'base_reserve', 'quote_reserve', 'block_height', 'tx_hash'
  ];
  await execBatchedInsert(
    client,
    'zigchain.dex_pools',
    cols,
    rows,
    'ON CONFLICT (pool_id) DO UPDATE SET ' +
    'creator_address = EXCLUDED.creator_address, ' +
    'pair_id = EXCLUDED.pair_id, ' +
    'base_denom = EXCLUDED.base_denom, ' +
    'quote_denom = EXCLUDED.quote_denom, ' +
    'lp_token_denom = EXCLUDED.lp_token_denom, ' +
    'base_reserve = EXCLUDED.base_reserve, ' +
    'quote_reserve = EXCLUDED.quote_reserve, ' +
    'block_height = EXCLUDED.block_height, ' +
    'tx_hash = EXCLUDED.tx_hash'
  );
}

// 3. DEX Swaps
export async function insertDexSwaps(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'pool_id', 'sender_address', 'token_in_denom',
    'token_in_amount', 'token_out_denom', 'token_out_amount', 
    'pair_id', 'effective_price', 'total_fee', 'price_impact', 
    'block_height', 'timestamp'
  ];
  await execBatchedInsert(
    client,
    'zigchain.dex_swaps',
    cols,
    rows,
    'ON CONFLICT (tx_hash, msg_index, block_height) DO NOTHING'
  );
}

// 4. DEX Liquidity
export async function insertDexLiquidity(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'pool_id', 'sender_address', 'action_type',
    'amount_0', 'amount_1', 'shares_minted_burned', 'block_height'
  ];
  await execBatchedInsert(
    client,
    'zigchain.dex_liquidity',
    cols,
    rows,
    'ON CONFLICT (tx_hash, msg_index, block_height) DO NOTHING'
  );
}

// 5. Wrapper Settings
export async function insertWrapperSettings(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'native_client_id', 'counterparty_client_id', 'native_port',
    'counterparty_port', 'native_channel', 'counterparty_channel',
    'decimal_difference', 'updated_at_height'
  ];
  await execBatchedInsert(
    client,
    'zigchain.wrapper_settings',
    cols,
    rows,
    'ON CONFLICT (denom) DO UPDATE SET ' +
    'native_client_id = EXCLUDED.native_client_id, ' +
    'counterparty_client_id = EXCLUDED.counterparty_client_id, ' +
    'native_port = EXCLUDED.native_port, ' +
    'counterparty_port = EXCLUDED.counterparty_port, ' +
    'native_channel = EXCLUDED.native_channel, ' +
    'counterparty_channel = EXCLUDED.counterparty_channel, ' +
    'decimal_difference = EXCLUDED.decimal_difference, ' +
    'updated_at_height = EXCLUDED.updated_at_height'
  );
}

// 6. Wrapper Events (JSONB metadata storage for bridge events)
export async function insertWrapperEvents(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'height', 'tx_hash', 'msg_index', 'sender', 'action',
    'amount', 'denom', 'metadata'
  ];
  await execBatchedInsert(
    client,
    'zigchain.wrapper_events',
    cols,
    rows,
    'ON CONFLICT (height, tx_hash, msg_index, action) DO NOTHING'
  );
}
