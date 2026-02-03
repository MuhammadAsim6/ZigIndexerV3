-- initdb/075-gov-v1-metadata-and-tally.sql

-- Add columns to gov.proposals for v1 support and final results
ALTER TABLE gov.proposals ADD COLUMN IF NOT EXISTS metadata TEXT;
ALTER TABLE gov.proposals ADD COLUMN IF NOT EXISTS tally_result JSONB;
ALTER TABLE gov.proposals ADD COLUMN IF NOT EXISTS executor_result TEXT;

-- Add metadata to gov.votes for v1 support
ALTER TABLE gov.votes ADD COLUMN IF NOT EXISTS metadata TEXT;
