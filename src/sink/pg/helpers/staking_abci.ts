/**
 * Helper for fetching staking validator metadata via ABCI query.
 */
import { RpcClient } from '../../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../../decode/dynamicProto.js';
import { getLogger } from '../../../utils/logger.js';
import { deriveConsensusAddress } from '../../../utils/crypto.js';
import { parseDec } from '../parsing.js';

const log = getLogger('staking/abci');

export interface ValidatorMetadata {
    operator_address: string;
    consensus_address: string | null;
    consensus_pubkey: string | null;
    moniker: string | null;
    website: string | null;
    details: string | null;
    commission_rate: string | null;
    max_commission_rate: string | null;
    max_change_rate: string | null;
    min_self_delegation: string | null;
    status: string | null;
}

/**
 * Fetches all validators from the chain via ABCI query.
 * @param rpc - RPC client
 * @param protoRoot - Protobuf Root for encoding/decoding
 * @returns Array of validator metadata
 */
export async function fetchAllValidatorsViaAbci(
    rpc: RpcClient,
    protoRoot: Root
): Promise<ValidatorMetadata[]> {
    try {
        const ReqType = protoRoot.lookupType('cosmos.staking.v1beta1.QueryValidatorsRequest');
        const ResType = 'cosmos.staking.v1beta1.QueryValidatorsResponse';

        const reqMsg = ReqType.create({
            status: '', // All statuses
            pagination: { limit: 500 }
        });
        const reqBytes = ReqType.encode(reqMsg).finish();
        const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

        const path = '/cosmos.staking.v1beta1.Query/Validators';
        const response = await rpc.queryAbci(path, reqHex);

        if (!response || response.code !== 0 || !response.value) {
            return [];
        }

        const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), protoRoot) as any;
        const validators = decoded?.validators || [];

        return validators.map((v: any) => {
            const pubkey = v.consensusPubkey?.value || v.consensus_pubkey?.value;
            const pubkeyStr = pubkey ? (typeof pubkey === 'string' ? pubkey : Buffer.from(pubkey).toString('base64')) : null;
            return {
                operator_address: v.operatorAddress || v.operator_address,
                consensus_address: pubkeyStr ? deriveConsensusAddress(pubkeyStr) : null,
                consensus_pubkey: pubkeyStr,
                moniker: v.description?.moniker || null,
                website: v.description?.website || null,
                details: v.description?.details || null,
                commission_rate: parseDec(v.commission?.commissionRates?.rate || v.commission?.commission_rates?.rate),
                max_commission_rate: parseDec(v.commission?.commissionRates?.maxRate || v.commission?.commission_rates?.max_rate),
                max_change_rate: parseDec(v.commission?.commissionRates?.maxChangeRate || v.commission?.commission_rates?.max_change_rate),
                min_self_delegation: v.minSelfDelegation || v.min_self_delegation || null,
                status: v.status || null,
            };
        });
    } catch (err: any) {
        log.warn(`[staking] Failed to fetch all validators via ABCI: ${err.message}`);
        return [];
    }
}
