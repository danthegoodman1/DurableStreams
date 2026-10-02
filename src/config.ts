import type { CompactionLimits } from "./compaction"

export type StreamConfig = {
	/** How long published records wait in memory so concurrent publishes share one segment. */
	flushIntervalMs: number
	compaction: CompactionLimits
	/** Longest wait for an R2 upload, a read's R2 requests, or a batch delete. */
	r2TimeoutMs: number
	/** Longest one compaction merge may take. Merged segments are under twice the size limits. */
	compactionTimeoutMs: number
}

export function readConfig(env: Env): StreamConfig {
	return {
		flushIntervalMs: readInteger(env, "FLUSH_INTERVAL_MS", 200, 0),
		compaction: {
			maxSegments: readInteger(env, "COMPACTION_MAX_SEGMENTS", 10, 2),
			maxRecords: readInteger(env, "COMPACTION_MAX_RECORDS", 5_000, 1),
			maxBytes: readInteger(env, "COMPACTION_MAX_BYTES", 10_000_000, 1),
		},
		r2TimeoutMs: 30_000,
		compactionTimeoutMs: 5 * 60_000,
	}
}

type ConfigVar = "FLUSH_INTERVAL_MS" | "COMPACTION_MAX_SEGMENTS" | "COMPACTION_MAX_RECORDS" | "COMPACTION_MAX_BYTES"

function readInteger(env: Env, name: ConfigVar, fallback: number, min: number): number {
	const raw = env[name]
	if (raw === undefined || raw === "") {
		return fallback
	}
	const value = Number(raw)
	if (!Number.isSafeInteger(value) || value < min) {
		throw new Error(`${name} must be an integer >= ${min}, got ${JSON.stringify(raw)}`)
	}
	return value
}
