// Optional variables and secrets. `wrangler types` generates only the bindings declared in wrangler.jsonc.
interface Env {
	AUTH_HEADER?: string
	FLUSH_INTERVAL_MS?: string
	COMPACTION_MAX_SEGMENTS?: string
	COMPACTION_MAX_RECORDS?: string
	COMPACTION_MAX_BYTES?: string
}
