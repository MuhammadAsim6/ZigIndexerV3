/**
 * Helper for fetching governance proposal data via ABCI query.
 * Used at indexing time to get accurate timestamps for proposals.
 */
import { RpcClient } from '../../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../../decode/dynamicProto.js';
import Long from 'long';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('gov/abci');

/**
 * Fetches proposal data from the chain via ABCI query.
 * @param rpc - RPC client
 * @param protoRoot - Protobuf Root for encoding/decoding
 * @param proposalId - Proposal ID to fetch
 * @returns Proposal data or null if not found
 */
export async function fetchProposalDataViaAbci(
    rpc: RpcClient,
    protoRoot: Root,
    proposalId: string | number
): Promise<{
    voting_start_time: Date | null;
    voting_end_time: Date | null;
    deposit_end_time: Date | null;
    submit_time: Date | null;
    title: string | null;
    summary: string | null;
} | null> {
    try {
        // Try v1 first
        const result = await fetchProposalViaAbciV1(rpc, protoRoot, proposalId);
        if (result) {
            log.info(`[gov] Fetched accurate data via ABCI for proposal ${proposalId}`);
            return result;
        }

        // Fallback to v1beta1
        const resultBeta = await fetchProposalViaAbciV1Beta1(rpc, protoRoot, proposalId);
        if (resultBeta) {
            log.info(`[gov] Fetched accurate data via ABCI (v1beta1) for proposal ${proposalId}`);
            return resultBeta;
        }

        log.warn(`[gov] Proposal ${proposalId} not found via ABCI`);
        return null;
    } catch (err: any) {
        log.warn(`[gov] Failed to fetch proposal ${proposalId} via ABCI: ${err.message}`);
        return null;
    }
}

async function fetchProposalViaAbciV1(
    rpc: RpcClient,
    root: Root,
    proposalId: string | number
): Promise<{
    voting_start_time: Date | null;
    voting_end_time: Date | null;
    deposit_end_time: Date | null;
    submit_time: Date | null;
    title: string | null;
    summary: string | null;
} | null> {
    try {
        const ReqType = root.lookupType('cosmos.gov.v1.QueryProposalRequest');
        const ResType = 'cosmos.gov.v1.QueryProposalResponse';

        // Encode request with proposal_id
        const reqMsg = ReqType.create({ proposalId: Long.fromValue(proposalId) });
        const reqBytes = ReqType.encode(reqMsg).finish();
        const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

        // Query ABCI
        const path = '/cosmos.gov.v1.Query/Proposal';
        const response = await rpc.queryAbci(path, reqHex);

        if (!response || response.code !== 0 || !response.value) {
            return null;
        }

        // Decode response
        const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), root) as any;
        const proposal = decoded?.proposal;

        if (!proposal) return null;

        return {
            voting_start_time: parseProtoTimestamp(proposal.votingStartTime),
            voting_end_time: parseProtoTimestamp(proposal.votingEndTime),
            deposit_end_time: parseProtoTimestamp(proposal.depositEndTime),
            submit_time: parseProtoTimestamp(proposal.submitTime),
            title: proposal.title || null,
            summary: proposal.summary || null,
        };
    } catch {
        return null;
    }
}

async function fetchProposalViaAbciV1Beta1(
    rpc: RpcClient,
    root: Root,
    proposalId: string | number
): Promise<{
    voting_start_time: Date | null;
    voting_end_time: Date | null;
    deposit_end_time: Date | null;
    submit_time: Date | null;
    title: string | null;
    summary: string | null;
} | null> {
    try {
        const ReqType = root.lookupType('cosmos.gov.v1beta1.QueryProposalRequest');
        const ResType = 'cosmos.gov.v1beta1.QueryProposalResponse';

        // Encode request
        const reqMsg = ReqType.create({ proposalId: Long.fromValue(proposalId) });
        const reqBytes = ReqType.encode(reqMsg).finish();
        const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

        // Query ABCI
        const path = '/cosmos.gov.v1beta1.Query/Proposal';
        const response = await rpc.queryAbci(path, reqHex);

        if (!response || response.code !== 0 || !response.value) {
            return null;
        }

        // Decode response
        const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), root) as any;
        const proposal = decoded?.proposal;

        if (!proposal) return null;

        return {
            voting_start_time: parseProtoTimestamp(proposal.votingStartTime),
            voting_end_time: parseProtoTimestamp(proposal.votingEndTime),
            deposit_end_time: parseProtoTimestamp(proposal.depositEndTime),
            submit_time: parseProtoTimestamp(proposal.submitTime),
            title: proposal.content?.title || null,
            summary: proposal.content?.description || null,
        };
    } catch {
        return null;
    }
}

/**
 * Parses protobuf Timestamp to Date.
 * Handles both { seconds, nanos } object format and string ISO format.
 */
function parseProtoTimestamp(val: any): Date | null {
    if (!val) return null;

    // Handle string ISO format
    if (typeof val === 'string') {
        const d = new Date(val);
        return isNaN(d.getTime()) ? null : d;
    }

    // Handle protobuf Timestamp { seconds, nanos }
    if (val.seconds !== undefined) {
        const seconds = typeof val.seconds === 'object' && val.seconds.toNumber
            ? val.seconds.toNumber()
            : Number(val.seconds);
        const nanos = Number(val.nanos || 0);
        return new Date(seconds * 1000 + Math.floor(nanos / 1000000));
    }

    return null;
}
