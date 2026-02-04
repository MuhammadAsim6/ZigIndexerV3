// src/utils/gov_params.ts
/**
 * Governance parameters helper for calculating proposal timestamps.
 * 
 * When event attributes don't include timestamps (common in Cosmos SDK v0.47+),
 * we calculate them using governance params and submit_time.
 */

/**
 * Default governance params from genesis.json.
 * These are used as fallbacks when calculating proposal timestamps.
 */
const DEFAULT_GOV_PARAMS = {
    // 1209600s = 14 days
    maxDepositPeriodMs: 1209600 * 1000,
    // 345600s = 4 days
    votingPeriodMs: 345600 * 1000,
    // 86400s = 1 day (for expedited proposals)
    expeditedVotingPeriodMs: 86400 * 1000,
};

// Cached params (can be updated at runtime if needed)
let govParams = { ...DEFAULT_GOV_PARAMS };

/**
 * Parse a Cosmos duration string like "345600s" to milliseconds.
 * @param dur - Duration string in format "Ns" where N is seconds
 * @returns Duration in milliseconds
 */
export function parseDurationToMs(dur: string | null | undefined): number | null {
    if (!dur) return null;
    const match = dur.match(/^(\d+)s$/);
    if (!match) return null;
    return parseInt(match[1], 10) * 1000;
}

/**
 * Update the cached governance params.
 * Call this if params are updated via MsgUpdateParams.
 */
export function updateGovParams(params: {
    maxDepositPeriodMs?: number;
    votingPeriodMs?: number;
    expeditedVotingPeriodMs?: number;
}): void {
    govParams = { ...govParams, ...params };
}

/**
 * Get current governance params.
 */
export function getGovParams() {
    return { ...govParams };
}

/**
 * Calculate deposit_end from submit_time.
 * deposit_end = submit_time + max_deposit_period
 */
export function calculateDepositEnd(submitTime: Date): Date {
    return new Date(submitTime.getTime() + govParams.maxDepositPeriodMs);
}

/**
 * Calculate voting_end from voting_start.
 * voting_end = voting_start + voting_period
 */
export function calculateVotingEnd(votingStart: Date, expedited = false): Date {
    const periodMs = expedited ? govParams.expeditedVotingPeriodMs : govParams.votingPeriodMs;
    return new Date(votingStart.getTime() + periodMs);
}

/**
 * Calculate a complete set of proposal dates from minimal information.
 * This is useful when only submit_time is known.
 * 
 * @param submitTime - When the proposal was submitted
 * @param isVotingPeriod - Whether the proposal is in voting period
 * @param votingStartOverride - Explicit voting_start if known from events
 * @param expedited - Whether this is an expedited proposal
 * @returns Calculated timestamps
 */
export function calculateProposalDates(
    submitTime: Date,
    isVotingPeriod: boolean,
    votingStartOverride?: Date | null,
    expedited = false
): {
    deposit_end: Date;
    voting_start: Date | null;
    voting_end: Date | null;
} {
    const deposit_end = calculateDepositEnd(submitTime);

    if (!isVotingPeriod) {
        // Still in deposit period
        return { deposit_end, voting_start: null, voting_end: null };
    }

    // In voting period - calculate voting dates
    const voting_start = votingStartOverride || submitTime; // If no explicit start, use submit time
    const voting_end = calculateVotingEnd(voting_start, expedited);

    return { deposit_end, voting_start, voting_end };
}
