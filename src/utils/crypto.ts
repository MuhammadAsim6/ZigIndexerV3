import crypto from 'node:crypto';
import { base64ToBytes, bytesToHex } from './bytes.js';

/**
 * Derives a Tendermint consensus address (Hex) from a base64-encoded Ed25519 public key.
 * 
 * Logic: 
 * 1. Decode base64 pubkey.
 * 2. Compute SHA-256 hash.
 * 3. Take the first 20 bytes (160 bits).
 * 4. Return as uppercase Hex string.
 * 
 * @param pubkeyBase64 - The Ed25519 public key in base64 format.
 * @returns The derived consensus address as an uppercase Hex string.
 */
export function deriveConsensusAddress(pubkeyBase64: string): string | null {
    if (!pubkeyBase64) return null;
    try {
        let bytes = base64ToBytes(pubkeyBase64);

        // Strip protobuf framing if present.
        // Protobuf-encoded Ed25519 PubKey has a 2-byte prefix: 0x0a (field 1, wire type 2) + 0x20 (length 32).
        // CometBFT derives the address from the RAW 32-byte key, not the protobuf envelope.
        if (bytes.length === 34 && bytes[0] === 0x0a && bytes[1] === 0x20) {
            bytes = bytes.slice(2);
        }

        const hash = crypto.createHash('sha256').update(bytes).digest();
        const truncated = hash.slice(0, 20);
        return bytesToHex(truncated).toUpperCase();
    } catch (e) {
        return null;
    }
}
