// src/sink/pg/flushers/gov.ts
import { PoolClient } from 'pg';
import { execBatchedInsert } from '../batch.js';

/**
 * Insert governance deposits in batches.
 *
 * @param client - Postgres client.
 * @param rows - Normalized deposit rows.
 */
export async function flushGovDeposits(
  client: PoolClient,
  rows: Array<{
    proposal_id: bigint;
    depositor: string;
    denom: string;
    amount: string;
    height: number;
    tx_hash: string;
    msg_index: number;
  }>,
) {
  if (!rows.length) return;

  const columns = ['proposal_id', 'depositor', 'denom', 'amount', 'height', 'tx_hash', 'msg_index'] as const;

  const shaped = rows.map((r) => ({
    proposal_id: r.proposal_id.toString(),
    depositor: r.depositor,
    denom: r.denom,
    amount: r.amount,
    height: r.height,
    tx_hash: r.tx_hash,
    msg_index: r.msg_index,
  }));

  await execBatchedInsert(client, 'gov.deposits', columns as unknown as string[], shaped, 'ON CONFLICT DO NOTHING');
}

/**
 * Insert governance votes in batches.
 *
 * For weighted votes, `weight` contains the decimal weight for the first option; for simple votes it is null.
 *
 * @param client - Postgres client.
 * @param rows - Normalized vote rows.
 */
export async function flushGovVotes(
  client: PoolClient,
  rows: Array<{
    proposal_id: bigint;
    voter: string;
    option: string;
    weight: string | null;
    height: number;
    tx_hash: string;
  }>,
) {
  if (!rows.length) return;

  const columns = ['proposal_id', 'voter', 'option', 'weight', 'height', 'tx_hash'] as const;

  const shaped = rows.map((r) => ({
    proposal_id: r.proposal_id.toString(),
    voter: r.voter,
    option: r.option,
    weight: r.weight,
    height: r.height,
    tx_hash: r.tx_hash,
  }));

  await execBatchedInsert(client, 'gov.votes', columns as unknown as string[], shaped, 'ON CONFLICT DO NOTHING');
}

/**
 * Upsert base information about proposals.
 *
 * This stores the basic record on submit (status is defaulted to 'deposit_period'
 * if not provided). Enrichment of status/lifecycle timestamps can be done later
 * by a background job.
 *
 * @param client - Postgres client.
 * @param rows - Proposal rows to upsert.
 */
export async function upsertGovProposals(
  client: PoolClient,
  rows: Array<{
    proposal_id: bigint;
    submitter: string | null;
    title: string | null;
    summary: string | null;
    proposal_type: string | null;
    status: string | null;
    submit_time: Date | null;
    deposit_end?: Date | null;
    voting_start?: Date | null;
    voting_end?: Date | null;
    total_deposit?: any | null;
    changes?: any | null;
  }>,
) {
  if (!rows.length) return;

  // 🛡️ PRE-MERGE: Prevent "ON CONFLICT DO UPDATE command cannot affect row a second time"
  // **FIX**: Merge values from multiple rows for the same proposal_id, preferring non-null values
  const mergedMap = new Map<string, any>();
  for (const row of rows) {
    const key = row.proposal_id.toString();
    const existing = mergedMap.get(key);
    if (existing) {
      // Merge: prefer new non-null values over existing null values
      mergedMap.set(key, {
        proposal_id: row.proposal_id,
        submitter: row.submitter ?? existing.submitter,
        title: row.title ?? existing.title,
        summary: row.summary ?? existing.summary,
        proposal_type: row.proposal_type ?? existing.proposal_type,
        status: row.status ?? existing.status, // Latest status wins (but don't downgrade with null)
        submit_time: row.submit_time ?? existing.submit_time,
        deposit_end: row.deposit_end ?? existing.deposit_end,
        voting_start: row.voting_start ?? existing.voting_start,
        voting_end: row.voting_end ?? existing.voting_end,
        total_deposit: row.total_deposit ?? existing.total_deposit,
        changes: row.changes ?? existing.changes,
      });
    } else {
      mergedMap.set(key, { ...row });
    }
  }
  const finalRows = Array.from(mergedMap.values());

  const columns = [
    'proposal_id', 'submitter', 'title', 'summary', 'proposal_type',
    'status', 'submit_time', 'deposit_end', 'voting_start', 'voting_end',
    'total_deposit', 'changes'
  ] as const;

  const shaped = finalRows.map((r) => ({
    proposal_id: r.proposal_id.toString(),
    submitter: r.submitter,
    title: r.title,
    summary: r.summary,
    proposal_type: r.proposal_type,
    status: r.status ?? 'deposit_period',
    submit_time: r.submit_time ? r.submit_time.toISOString() : null,
    deposit_end: r.deposit_end ? r.deposit_end.toISOString() : null,
    voting_start: r.voting_start ? r.voting_start.toISOString() : null,
    voting_end: r.voting_end ? r.voting_end.toISOString() : null,
    total_deposit: r.total_deposit ? JSON.stringify(r.total_deposit) : null,
    changes: r.changes ? JSON.stringify(r.changes) : null,
  }));

  await execBatchedInsert(
    client,
    'gov.proposals',
    columns as unknown as string[],
    shaped,
    `ON CONFLICT (proposal_id) DO UPDATE SET
      submitter       = COALESCE(EXCLUDED.submitter, gov.proposals.submitter),
      title           = COALESCE(EXCLUDED.title, gov.proposals.title),
      summary         = COALESCE(EXCLUDED.summary, gov.proposals.summary),
      proposal_type   = COALESCE(EXCLUDED.proposal_type, gov.proposals.proposal_type),
      status          = EXCLUDED.status,
      submit_time     = COALESCE(EXCLUDED.submit_time, gov.proposals.submit_time),
      deposit_end     = COALESCE(EXCLUDED.deposit_end, gov.proposals.deposit_end),
      voting_start    = COALESCE(EXCLUDED.voting_start, gov.proposals.voting_start),
      voting_end      = COALESCE(EXCLUDED.voting_end, gov.proposals.voting_end),
      total_deposit   = COALESCE(EXCLUDED.total_deposit, gov.proposals.total_deposit),
      changes         = COALESCE(EXCLUDED.changes, gov.proposals.changes)`,
  );
}
