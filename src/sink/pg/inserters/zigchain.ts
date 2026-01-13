import type { PoolClient } from 'pg';
import { makeMultiInsert } from '../batch.js';

// 1. Factory Denoms
export async function insertFactoryDenoms(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'creator_address', 'sub_denom', 'minting_cap', 
    'uri', 'uri_hash', 'description', 'creation_tx_hash', 'block_height'
  ];
  const { text, values } = makeMultiInsert(
    'zigchain.factory_denoms',
    cols,
    rows,
    'ON CONFLICT (denom) DO NOTHING'
  );
  await client.query(text, values);
}

// 2. DEX Pools (Updated with Reserves & Pair ID)
export async function insertDexPools(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'pool_id', 'creator_address', 'pair_id', 
    'base_denom', 'quote_denom', 'lp_token_denom', 
    'base_reserve', 'quote_reserve', // 👈 Added for Analytics
    'block_height', 'tx_hash'
  ];
  const { text, values } = makeMultiInsert(
    'zigchain.dex_pools',
    cols,
    rows,
    'ON CONFLICT (pool_id) DO NOTHING'
  );
  await client.query(text, values);
}

// 3. DEX Swaps
export async function insertDexSwaps(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'pool_id', 'sender_address',
    'token_in_denom', 'token_in_amount', 'token_out_denom', 'token_out_amount',
    'block_height'
  ];
  const { text, values } = makeMultiInsert(
    'zigchain.dex_swaps',
    cols,
    rows,
    'ON CONFLICT DO NOTHING'
  );
  await client.query(text, values);
}

// 4. DEX Liquidity (🆕 NEW FUNCTION)
export async function insertDexLiquidity(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'tx_hash', 'msg_index', 'pool_id', 'sender_address',
    'action_type', // 'ADD' or 'REMOVE'
    'amount_0', 'amount_1', 'shares_minted_burned',
    'block_height'
  ];
  const { text, values } = makeMultiInsert(
    'zigchain.dex_liquidity',
    cols,
    rows,
    'ON CONFLICT DO NOTHING'
  );
  await client.query(text, values);
}

// 5. Wrapper Settings
export async function insertWrapperSettings(client: PoolClient, rows: any[]): Promise<void> {
  if (!rows?.length) return;
  const cols = [
    'denom', 'native_client_id', 'native_channel', 
    'decimal_difference', 'updated_at_height'
  ];
  const { text, values } = makeMultiInsert(
    'zigchain.wrapper_settings',
    cols,
    rows,
    'ON CONFLICT (denom) DO UPDATE SET updated_at_height = EXCLUDED.updated_at_height'
  );
  await client.query(text, values);
}